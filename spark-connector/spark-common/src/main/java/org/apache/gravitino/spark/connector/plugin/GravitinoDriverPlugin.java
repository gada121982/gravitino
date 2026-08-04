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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.auth.AuthProperties;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.client.GravitinoClient.ClientBuilder;
import org.apache.gravitino.client.GravitinoClientConfiguration;
import org.apache.gravitino.client.KerberosTokenProvider;
import org.apache.gravitino.client.SessionAwareOAuth2TokenProvider;
import org.apache.gravitino.spark.connector.GravitinoSparkConfig;
import org.apache.gravitino.spark.connector.catalog.GravitinoCatalogManager;
import org.apache.gravitino.spark.connector.iceberg.extensions.GravitinoIcebergSparkSessionExtensions;
import org.apache.gravitino.spark.connector.version.CatalogNameAdaptor;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.plugin.DriverPlugin;
import org.apache.spark.api.plugin.PluginContext;
import org.apache.spark.sql.internal.StaticSQLConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GravitinoDriverPlugin creates GravitinoCatalogManager to fetch catalogs from Apache Gravitino and
 * register Gravitino catalogs to Apache Spark.
 */
public class GravitinoDriverPlugin implements DriverPlugin {

  private static final Logger LOG = LoggerFactory.getLogger(GravitinoDriverPlugin.class);

  @VisibleForTesting
  static final String PAIMON_SPARK_EXTENSIONS =
      "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions";

  @VisibleForTesting
  static final String ICEBERG_SPARK_EXTENSIONS =
      "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions";

  private static final String ICEBERG_PROVIDER = "lakehouse-iceberg";
  private static final String PAIMON_PROVIDER = "lakehouse-paimon";
  private static final String SPARK_CATALOG_PREFIX = "spark.sql.catalog.";
  private static final String ICEBERG_REST_CATALOG_CLASS = "org.apache.iceberg.spark.SparkCatalog";
  private static final String ICEBERG_ACCESS_DELEGATION_HEADER =
      "header.X-Iceberg-Access-Delegation";
  private static final String ICEBERG_REMOTE_SIGNING = "remote-signing";

  private GravitinoCatalogManager catalogManager;
  private final List<String> gravitinoIcebergExtensions =
      Arrays.asList(
          GravitinoIcebergSparkSessionExtensions.class.getName(), ICEBERG_SPARK_EXTENSIONS);
  private final List<String> gravitinoPaimonExtensions = Arrays.asList(PAIMON_SPARK_EXTENSIONS);

  private final List<String> gravitinoDriverExtensions = new ArrayList<>();
  private boolean enableIcebergSupport = false;
  private boolean enablePaimonSupport = false;
  private boolean icebergRestEagerRegister = false;
  private String icebergRestUri = "";
  private String metalakeName = "";

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

    this.enableIcebergSupport =
        conf.getBoolean(GravitinoSparkConfig.GRAVITINO_ENABLE_ICEBERG_SUPPORT, false);
    this.enablePaimonSupport =
        conf.getBoolean(GravitinoSparkConfig.GRAVITINO_ENABLE_PAIMON_SUPPORT, false);
    if (enablePaimonSupport) {
      gravitinoDriverExtensions.addAll(gravitinoPaimonExtensions);
    }
    if (enableIcebergSupport) {
      gravitinoDriverExtensions.addAll(gravitinoIcebergExtensions);
    }

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
            () ->
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

  private void registerGravitinoCatalogs(
      SparkConf sparkConf, Map<String, Catalog> gravitinoCatalogs) {
    gravitinoCatalogs
        .entrySet()
        .forEach(
            entry -> {
              String catalogName = entry.getKey();
              Catalog gravitinoCatalog = entry.getValue();
              String provider = gravitinoCatalog.provider();
              boolean isIceberg = ICEBERG_PROVIDER.equals(provider.toLowerCase(Locale.ROOT));
              if (isIceberg && !icebergRestEagerRegister && !enableIcebergSupport) {
                return;
              }
              if (PAIMON_PROVIDER.equals(provider.toLowerCase(Locale.ROOT))
                  && !enablePaimonSupport) {
                return;
              }
              try {
                if (isIceberg && icebergRestEagerRegister) {
                  registerIcebergRestCatalog(sparkConf, catalogName);
                } else {
                  registerCatalog(sparkConf, catalogName, provider);
                }
              } catch (Exception e) {
                LOG.warn("Register catalog {} failed.", catalogName, e);
              }
            });
  }

  private void registerCatalog(SparkConf sparkConf, String catalogName, String provider) {
    if (StringUtils.isBlank(provider)) {
      LOG.warn("Skip registering {} because catalog provider is empty.", catalogName);
      return;
    }

    String catalogClassName = CatalogNameAdaptor.getCatalogName(provider);
    if (StringUtils.isBlank(catalogClassName)) {
      LOG.warn("Skip registering {} because {} is not supported yet.", catalogName, provider);
      return;
    }

    String sparkCatalogConfigName = SPARK_CATALOG_PREFIX + catalogName;
    Preconditions.checkArgument(
        !sparkConf.contains(sparkCatalogConfigName),
        catalogName + " is already registered to SparkCatalogManager");
    sparkConf.set(sparkCatalogConfigName, catalogClassName);
    LOG.info("Register {} catalog to Spark catalog manager.", catalogName);
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

  private void registerSqlExtensions(SparkConf conf) {
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
