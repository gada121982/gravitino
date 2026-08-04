/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.spark.connector.plugin;

import static org.apache.gravitino.spark.connector.ConnectorConstants.COMMA;
import static org.apache.gravitino.spark.connector.GravitinoSparkConfig.GRAVITINO_CLIENT_CONFIG_PREFIX;
import static org.apache.gravitino.spark.connector.utils.ConnectorUtil.removeDuplicateSparkExtensions;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.auth.AuthProperties;
import org.apache.gravitino.client.CustomTokenProvider;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.client.GravitinoClient.ClientBuilder;
import org.apache.gravitino.client.GravitinoClientConfiguration;
import org.apache.gravitino.client.KerberosTokenProvider;
import org.apache.gravitino.client.SessionAwareOAuth2TokenProvider;
import org.apache.gravitino.spark.connector.GravitinoSparkConfig;
import org.apache.gravitino.spark.connector.catalog.GravitinoCatalogManager;
import org.apache.gravitino.spark.connector.catalog.SparkCatalogKind;
import org.apache.gravitino.spark.connector.iceberg.extensions.GravitinoIcebergSparkSessionExtensions;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.plugin.DriverPlugin;
import org.apache.spark.api.plugin.PluginContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.StaticSQLConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

/**
 * GravitinoDriverPlugin creates GravitinoCatalogManager to fetch catalogs from Apache Gravitino and
 * register Gravitino catalogs to Apache Spark.
 *
 * <p>The classes it registers differ per Spark version, so it takes them as {@link SparkBindings}
 * from the version module's {@code GravitinoSparkPlugin} rather than naming them itself.
 */
public class GravitinoDriverPlugin implements DriverPlugin {

  private static final Logger LOG = LoggerFactory.getLogger(GravitinoDriverPlugin.class);

  @VisibleForTesting static final String PAIMON_PROVIDER = "lakehouse-paimon";

  @VisibleForTesting
  static final String PAIMON_SPARK_EXTENSIONS =
      "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions";

  @VisibleForTesting
  static final String ICEBERG_SPARK_EXTENSIONS =
      "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions";

  private static final String SPARK_CATALOG_PREFIX = "spark.sql.catalog.";
  private static final String ICEBERG_REST_CATALOG_CLASS = "org.apache.iceberg.spark.SparkCatalog";
  private static final String ICEBERG_ACCESS_DELEGATION_HEADER =
      "header.X-Iceberg-Access-Delegation";
  private static final String ICEBERG_REMOTE_SIGNING = "remote-signing";

  private final SparkBindings bindings;
  private final List<String> gravitinoIcebergExtensions =
      Arrays.asList(
          GravitinoIcebergSparkSessionExtensions.class.getName(), ICEBERG_SPARK_EXTENSIONS);
  private final List<String> gravitinoPaimonExtensions = Arrays.asList(PAIMON_SPARK_EXTENSIONS);

  private final List<String> gravitinoDriverExtensions;
  private GravitinoCatalogManager catalogManager;
  private boolean enableIcebergSupport = false;
  private boolean enablePaimonSupport = false;
  private boolean icebergRestEagerRegister = false;
  private String icebergRestUri = "";
  private String metalakeName = "";

  /**
   * Creates the plugin from the classes a connector build supplies.
   *
   * @param bindings the catalog and session extension classes of this build
   */
  public GravitinoDriverPlugin(SparkBindings bindings) {
    this.bindings = bindings;
    this.gravitinoDriverExtensions =
        new ArrayList<>(Collections.singletonList(bindings.authorizationExtension()));
  }

  @Override
  public Map<String, String> init(SparkContext sc, PluginContext pluginContext) {
    SparkConf conf = sc.conf();
    String gravitinoUri = conf.get(GravitinoSparkConfig.GRAVITINO_URI);
    String metalake = conf.get(GravitinoSparkConfig.GRAVITINO_METALAKE);
    Map<String, String> gravitinoClientConfig = extractGravitinoClientConfig(conf);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(gravitinoUri),
        String.format(
            "%s:%s, should not be empty", GravitinoSparkConfig.GRAVITINO_URI, gravitinoUri));
    Preconditions.checkArgument(
        StringUtils.isNotBlank(metalake),
        String.format(
            "%s:%s, should not be empty", GravitinoSparkConfig.GRAVITINO_METALAKE, metalake));

    registerOptInExtensions(conf);

    // Iceberg catalogs can be registered as plain Iceberg REST catalogs pointed at Gravitino's
    // Iceberg REST service instead of going through this connector. Registering them here — while
    // the driver is still building its SparkContext — keeps every access path working, including
    // the DataFrame ones that never reach a SQL parser. Only enable this where the OAuth2
    // credential already in SparkConf belongs to the identity the job runs as; a shared engine
    // serving several users must keep resolving credentials per session instead.
    this.metalakeName = metalake;
    this.icebergRestUri = conf.get(GravitinoSparkConfig.GRAVITINO_ICEBERG_REST_URI, "").trim();
    this.icebergRestEagerRegister =
        conf.getBoolean(GravitinoSparkConfig.GRAVITINO_ICEBERG_REST_EAGER_REGISTER, false)
            && StringUtils.isNotBlank(icebergRestUri);
    if (icebergRestEagerRegister) {
      // The Iceberg SQL extensions are still required for Iceberg DDL and stored procedures;
      // the Gravitino-specific ones are not, since the catalog no longer uses the connector.
      gravitinoDriverExtensions.add(ICEBERG_SPARK_EXTENSIONS);
    }

    this.catalogManager =
        GravitinoCatalogManager.create(
            conf,
            sc.sparkUser(),
            identity ->
                createGravitinoClient(
                    gravitinoUri, metalake, conf, sc.sparkUser(), gravitinoClientConfig));
    catalogManager.loadRelationalCatalogs();
    registerGravitinoCatalogs(conf, catalogManager.getCatalogs());
    registerSqlExtensions(conf);
    return Collections.emptyMap();
  }

  @Override
  public void shutdown() {
    if (catalogManager != null) {
      catalogManager.close();
    }
  }

  @VisibleForTesting
  void registerGravitinoCatalogs(SparkConf sparkConf, Map<String, Catalog> gravitinoCatalogs) {
    gravitinoCatalogs
        .entrySet()
        .forEach(
            entry -> {
              String catalogName = entry.getKey();
              Catalog gravitinoCatalog = entry.getValue();
              String provider = gravitinoCatalog.provider();
              if (StringUtils.isBlank(provider)) {
                LOG.warn("Skip registering {} because catalog provider is empty.", catalogName);
                return;
              }
              // The provider is read once, here, and every decision below is made from the kind, so
              // no two places can disagree about what this catalog is.
              SparkCatalogKind kind = SparkCatalogKind.fromProvider(provider);
              if (kind == null) {
                LOG.warn(
                    "Skip registering {} because {} is not supported yet.", catalogName, provider);
                return;
              }
              if (SparkCatalogKind.LAKEHOUSE_ICEBERG.equals(kind)
                  && !icebergRestEagerRegister
                  && !enableIcebergSupport) {
                return;
              }
              if (SparkCatalogKind.LAKEHOUSE_PAIMON.equals(kind) && !enablePaimonSupport) {
                return;
              }
              String sparkCatalogConfigName = "spark.sql.catalog." + catalogName;
              if (sparkConf.contains(sparkCatalogConfigName)) {
                LOG.info(
                    "Skip registering catalog {} because it is already configured in Spark.",
                    catalogName);
                return;
              }
              try {
                if (SparkCatalogKind.LAKEHOUSE_ICEBERG.equals(kind) && icebergRestEagerRegister) {
                  registerIcebergRestCatalog(sparkConf, catalogName);
                } else {
                  registerCatalog(sparkConf, catalogName, kind);
                }
              } catch (Exception e) {
                LOG.warn("Register catalog {} failed.", catalogName, e);
              }
            });
  }

  /**
   * Registers the Spark catalog class bound to {@code kind} under {@code catalogName}, or skips the
   * catalog when this build bound none.
   *
   * @param sparkConf the conf to register into
   * @param catalogName the Gravitino catalog name, which becomes the Spark catalog name
   * @param kind the kind of catalog to register
   * @throws NullPointerException if the kind is null. A provider that maps to no kind is the
   *     caller's to report, since only the caller still has the provider name.
   */
  @VisibleForTesting
  void registerCatalog(SparkConf sparkConf, String catalogName, SparkCatalogKind kind) {
    String catalogClassName = catalogClassName(kind);
    if (catalogClassName == null) {
      LOG.warn("Skip registering {} because this build has no {} catalog.", catalogName, kind);
      return;
    }

    String sparkCatalogConfigName = SPARK_CATALOG_PREFIX + catalogName;
    Preconditions.checkArgument(
        !sparkConf.contains(sparkCatalogConfigName),
        catalogName + " is already registered to SparkCatalogManager");
    sparkConf.set(sparkCatalogConfigName, catalogClassName);
    LOG.info("Register {} catalog to Spark catalog manager.", catalogName);
  }

  /**
   * Resolves the Spark catalog class bound to a kind, from the bindings this build supplied.
   * Returns null when the build bound no catalog for it, which is how a kind this build cannot
   * serve, such as Paimon on Spark 4, is skipped.
   */
  @Nullable
  @VisibleForTesting
  String catalogClassName(SparkCatalogKind kind) {
    Objects.requireNonNull(kind, "Catalog kind must not be null");
    return bindings.catalogClassNames().get(kind);
  }

  /**
   * Reads the two opt-in flags and queues the session extensions they ask for. Extracted from
   * {@link #init(SparkContext, PluginContext)} so a test can exercise the flags without a live
   * SparkContext, and takes the conf rather than two booleans so the config keys are covered too.
   *
   * @param conf the Spark conf to read the opt-in flags from
   */
  @VisibleForTesting
  void registerOptInExtensions(SparkConf conf) {
    this.enableIcebergSupport =
        conf.getBoolean(GravitinoSparkConfig.GRAVITINO_ENABLE_ICEBERG_SUPPORT, false);
    this.enablePaimonSupport =
        conf.getBoolean(GravitinoSparkConfig.GRAVITINO_ENABLE_PAIMON_SUPPORT, false);
    if (enablePaimonSupport) {
      registerPaimonExtensionsIfSupported();
    }
    if (enableIcebergSupport) {
      gravitinoDriverExtensions.addAll(gravitinoIcebergExtensions);
    }
  }

  /**
   * Queues the Paimon session extensions, unless this build has no Paimon catalog. Paimon publishes
   * no paimon-spark artifact for every Spark version and Scala version this connector supports, so
   * some builds omit the Paimon classes; registering the extension there would fail SparkSession
   * construction with a ClassNotFoundException. Skip it the way an unsupported catalog provider is
   * skipped, so a config carried over from another build degrades to a warning.
   */
  @VisibleForTesting
  void registerPaimonExtensionsIfSupported() {
    if (!bindings.catalogClassNames().containsKey(SparkCatalogKind.LAKEHOUSE_PAIMON)) {
      LOG.warn(
          "Skip registering Paimon session extensions because {} is not supported yet.",
          PAIMON_PROVIDER);
      return;
    }
    gravitinoDriverExtensions.addAll(gravitinoPaimonExtensions);
  }

  private void registerIcebergRestCatalog(SparkConf sparkConf, String catalogName) {
    String sparkCatalogConfigName = SPARK_CATALOG_PREFIX + catalogName;
    Preconditions.checkArgument(
        !sparkConf.contains(sparkCatalogConfigName),
        catalogName + " is already registered to SparkCatalogManager");
    icebergRestCatalogConf(
            catalogName,
            metalakeName,
            icebergRestUri,
            sparkConf.get(GravitinoSparkConfig.GRAVITINO_OAUTH2_CREDENTIAL, ""),
            sparkConf.get(GravitinoSparkConfig.GRAVITINO_OAUTH2_URI, ""),
            sparkConf.get(GravitinoSparkConfig.GRAVITINO_OAUTH2_PATH, ""),
            sparkConf.get(GravitinoSparkConfig.GRAVITINO_OAUTH2_SCOPE, ""))
        .forEach(sparkConf::set);
    LOG.info(
        "Register {} catalog to Spark catalog manager as an Iceberg REST catalog.", catalogName);
  }

  /**
   * Builds the Spark conf entries that make {@code catalogName} an Iceberg REST catalog served by
   * Gravitino. The warehouse is metalake-qualified so a single REST endpoint can address catalogs
   * across metalakes, and access is delegated back to the server so the engine signs storage
   * requests through Gravitino rather than holding storage credentials itself.
   */
  @VisibleForTesting
  static Map<String, String> icebergRestCatalogConf(
      String catalogName,
      String metalake,
      String restUri,
      String credential,
      String oauth2ServerUri,
      String oauth2TokenPath,
      String oauth2Scope) {
    String prefix = SPARK_CATALOG_PREFIX + catalogName;
    Map<String, String> catalogConf = new LinkedHashMap<>();
    catalogConf.put(prefix, ICEBERG_REST_CATALOG_CLASS);
    catalogConf.put(prefix + ".type", "rest");
    catalogConf.put(prefix + ".uri", restUri);
    catalogConf.put(prefix + ".warehouse", metalake + "." + catalogName);
    catalogConf.put(prefix + "." + ICEBERG_ACCESS_DELEGATION_HEADER, ICEBERG_REMOTE_SIGNING);
    if (StringUtils.isNotBlank(credential)) {
      catalogConf.put(prefix + ".credential", credential);
    }
    if (StringUtils.isNotBlank(oauth2ServerUri)) {
      catalogConf.put(
          prefix + ".oauth2-server-uri",
          StringUtils.stripEnd(oauth2ServerUri, "/")
              + "/"
              + StringUtils.stripStart(oauth2TokenPath, "/"));
    }
    if (StringUtils.isNotBlank(oauth2Scope)) {
      catalogConf.put(prefix + ".scope", oauth2Scope);
    }
    return catalogConf;
  }

  @VisibleForTesting
  void registerSqlExtensions(SparkConf conf) {
    String extensionString = String.join(COMMA, gravitinoDriverExtensions);
    if (conf.contains(StaticSQLConf.SPARK_SESSION_EXTENSIONS().key())) {
      String sparkSessionExtensions = conf.get(StaticSQLConf.SPARK_SESSION_EXTENSIONS().key());
      if (StringUtils.isNotBlank(sparkSessionExtensions)) {
        conf.set(
            StaticSQLConf.SPARK_SESSION_EXTENSIONS().key(),
            removeDuplicateSparkExtensions(
                gravitinoDriverExtensions.toArray(new String[0]),
                sparkSessionExtensions.split(COMMA)));
      } else {
        conf.set(StaticSQLConf.SPARK_SESSION_EXTENSIONS().key(), extensionString);
      }
    } else {
      conf.set(StaticSQLConf.SPARK_SESSION_EXTENSIONS().key(), extensionString);
    }
  }

  /**
   * Creates a Gravitino client using authentication settings from the Spark configuration.
   *
   * @param uri Gravitino REST server URI
   * @param metalake Gravitino metalake name
   * @param sparkConf Spark configuration containing auth settings
   * @param sparkUser Spark session user for simple authentication
   * @param clientConfig additional Gravitino client configuration
   * @return configured Gravitino client
   */
  public static GravitinoClient createGravitinoClient(
      String uri,
      String metalake,
      SparkConf sparkConf,
      String sparkUser,
      Map<String, String> clientConfig) {
    ClientBuilder builder = GravitinoClient.builder(uri).withMetalake(metalake);
    builder.withClientConfig(clientConfig);
    String authType =
        sparkConf.get(GravitinoSparkConfig.GRAVITINO_AUTH_TYPE, AuthProperties.SIMPLE_AUTH_TYPE);
    if (AuthProperties.isSimple(authType)) {
      Preconditions.checkArgument(
          !UserGroupInformation.isSecurityEnabled(),
          "Spark simple auth mode doesn't support setting kerberos configurations");
      builder.withSimpleAuth(sparkUser);
    } else if (AuthProperties.isBasic(authType)) {
      String username = getRequiredConfig(sparkConf, GravitinoSparkConfig.GRAVITINO_BASIC_USERNAME);
      String password = getRequiredConfig(sparkConf, GravitinoSparkConfig.GRAVITINO_BASIC_PASSWORD);
      builder.withBasicAuth(username, password);
    } else if (AuthProperties.isOAuth2(authType)) {
      String oAuthUri = getRequiredConfig(sparkConf, GravitinoSparkConfig.GRAVITINO_OAUTH2_URI);
      String credential =
          getRequiredConfig(sparkConf, GravitinoSparkConfig.GRAVITINO_OAUTH2_CREDENTIAL);
      String path = getRequiredConfig(sparkConf, GravitinoSparkConfig.GRAVITINO_OAUTH2_PATH);
      String scope = getRequiredConfig(sparkConf, GravitinoSparkConfig.GRAVITINO_OAUTH2_SCOPE);
      SessionAwareOAuth2TokenProvider oAuth2TokenProvider =
          SessionAwareOAuth2TokenProvider.builder()
              .withUri(oAuthUri)
              .withCredential(credential)
              .withPath(path)
              .withScope(scope)
              .build();
      builder.withOAuth(oAuth2TokenProvider);
    } else if (AuthProperties.isKerberos(authType)) {
      String principal =
          getRequiredConfig(sparkConf, GravitinoSparkConfig.GRAVITINO_KERBEROS_PRINCIPAL);
      String keyTabFile =
          getRequiredConfig(sparkConf, GravitinoSparkConfig.GRAVITINO_KERBEROS_KEYTAB_FILE_PATH);
      KerberosTokenProvider kerberosTokenProvider =
          KerberosTokenProvider.builder()
              .withClientPrincipal(principal)
              .withKeyTabFile(new File(keyTabFile))
              .build();
      builder.withKerberosAuth(kerberosTokenProvider);
    } else if (AuthProperties.isToken(authType)) {
      builder.withCustomTokenAuth(new DynamicBearerTokenProvider(sparkConf));
    } else {
      throw new UnsupportedOperationException("Unsupported auth type: " + authType);
    }
    return builder.build();
  }

  private static String getRequiredConfig(SparkConf sparkConf, String configKey) {
    String configValue = sparkConf.get(configKey, null);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(configValue), configKey + " should not be empty");
    return configValue;
  }

  @Nullable
  private static String getOptionalConfig(SparkConf sparkConf, String configKey) {
    return sparkConf.get(configKey, null);
  }

  /**
   * Resolves the bearer token to present to Gravitino for the operation running on the current
   * thread.
   *
   * <p>The active Spark session's configuration wins over the application configuration, so a
   * shared driver can carry a different end user's token per session, and {@code tokenFile} wins
   * over {@code token}, so a token refreshed on disk is picked up without touching the
   * configuration. Neither the token nor any prefix of it is ever logged.
   *
   * @param sparkConf the application Spark configuration, used when no session supplies a token
   * @return the bearer token, never blank
   * @throws IllegalArgumentException if neither token property is set
   * @throws UncheckedIOException if the configured token file cannot be read
   */
  public static String resolveToken(SparkConf sparkConf) {
    String tokenFile =
        getSessionOrApplicationConfig(sparkConf, GravitinoSparkConfig.GRAVITINO_TOKEN_FILE);
    String token =
        StringUtils.isNotBlank(tokenFile)
            ? readTokenFile(tokenFile)
            : getSessionOrApplicationConfig(sparkConf, GravitinoSparkConfig.GRAVITINO_TOKEN_VALUE);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(token),
        String.format(
            "Either %s or %s should be set when %s is %s",
            GravitinoSparkConfig.GRAVITINO_TOKEN_VALUE,
            GravitinoSparkConfig.GRAVITINO_TOKEN_FILE,
            GravitinoSparkConfig.GRAVITINO_AUTH_TYPE,
            AuthProperties.TOKEN_AUTH_TYPE));
    return token.trim();
  }

  private static String readTokenFile(String tokenFile) {
    try {
      return new String(Files.readAllBytes(Paths.get(tokenFile)), StandardCharsets.UTF_8).trim();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read the Gravitino token file " + tokenFile, e);
    }
  }

  @Nullable
  private static String getSessionOrApplicationConfig(SparkConf sparkConf, String configKey) {
    Option<SparkSession> activeSession = SparkSession.getActiveSession();
    if (activeSession.isDefined()) {
      Option<String> sessionValue = activeSession.get().conf().getOption(configKey);
      if (sessionValue.isDefined() && StringUtils.isNotBlank(sessionValue.get())) {
        return sessionValue.get();
      }
    }
    return getOptionalConfig(sparkConf, configKey);
  }

  /**
   * Presents a bearer token that is resolved again on every request rather than captured once. See
   * {@link #resolveToken(SparkConf)} for where the token comes from.
   */
  @VisibleForTesting
  static final class DynamicBearerTokenProvider extends CustomTokenProvider {

    private final SparkConf sparkConf;

    DynamicBearerTokenProvider(SparkConf sparkConf) {
      this.sparkConf = sparkConf;
      this.schemeName = "Bearer";
    }

    @Override
    protected String getCustomTokenInfo() {
      return resolveToken(sparkConf);
    }
  }

  @VisibleForTesting
  public static Map<String, String> extractGravitinoClientConfig(SparkConf conf) {
    return Optional.ofNullable(conf.getAllWithPrefix(GRAVITINO_CLIENT_CONFIG_PREFIX))
        .map(
            arr ->
                Stream.of(arr)
                    .collect(
                        Collectors.toMap(
                            t -> GravitinoClientConfiguration.GRAVITINO_CLIENT_CONFIG_PREFIX + t._1,
                            t -> t._2,
                            (oldVal, newVal) -> newVal)))
        .orElse(ImmutableMap.of());
  }
}
