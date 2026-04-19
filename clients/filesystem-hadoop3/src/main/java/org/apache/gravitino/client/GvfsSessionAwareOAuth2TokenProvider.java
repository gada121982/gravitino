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
package org.apache.gravitino.client;

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.StringUtils;

/**
 * Session-aware OAuth2 token provider for GVFS. Resolves per-user OAuth2 credential from the active
 * SparkSession (set by gravitino-catalog-sync at session start), and falls back to the shared
 * credential otherwise.
 *
 * <p>Mirrors {@link SessionAwareOAuth2TokenProvider} (used by the Spark SQL connector), but the
 * filesystem-hadoop3 module cannot depend on Spark. Reflection is used to read SparkSession so this
 * provider stays safe to load in non-Spark environments (plain Hadoop, Flink, Trino, etc.).
 *
 * <p>Wiring: set {@code fs.gravitino.client.authType=session-oauth2} in Hadoop/Spark conf; {@link
 * org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystemUtils} will build the
 * GravitinoClient with this provider.
 */
public class GvfsSessionAwareOAuth2TokenProvider extends OAuth2TokenProvider {

  // Config key read from SparkSession per-session — CatalogSyncExtension writes it after resolving
  // the IAM user. Reusing the same key means SQL and GVFS share one per-user credential.
  private static final String SESSION_CREDENTIAL_KEY = "spark.sql.gravitino.oauth2.credential";

  private String sharedCredential;
  private String scope;
  private String path;

  // Token cache keyed by credential string — lets users in the same JVM each keep their own token
  // without re-fetching on every call.
  private final ConcurrentHashMap<String, String> tokenCache = new ConcurrentHashMap<>();

  private GvfsSessionAwareOAuth2TokenProvider() {}

  @Override
  protected synchronized String getAccessToken() {
    String credential = resolveCredential();
    if (StringUtils.isBlank(credential)) {
      // Fail-fast: no per-user credential in SparkSession and no shared fallback configured.
      // Returning null would make Gravitino silently use admin identity — refuse instead so
      // RBAC cannot be bypassed when session wiring is broken or called from non-Spark code.
      throw new SecurityException(
          "GvfsSessionAwareOAuth2TokenProvider: no per-user OAuth2 credential available "
              + "(spark.sql.gravitino.oauth2.credential not set in active SparkSession and no "
              + "shared fallback configured). Refusing to vend admin token.");
    }
    String token = tokenCache.get(credential);

    Long expires = OAuth2ClientUtil.expiresAtMillis(token);
    if (expires == null || expires <= System.currentTimeMillis()) {
      token =
          OAuth2ClientUtil.fetchToken(client, Collections.emptyMap(), credential, scope, path)
              .getAccessToken();
      tokenCache.put(credential, token);
    }
    return token;
  }

  private String resolveCredential() {
    try {
      Class<?> sparkSessionClass = Class.forName("org.apache.spark.sql.SparkSession");
      Object session = sparkSessionClass.getMethod("active").invoke(null);
      Object runtimeConf = session.getClass().getMethod("conf").invoke(session);
      String sessionCredential =
          (String)
              runtimeConf
                  .getClass()
                  .getMethod("get", String.class, String.class)
                  .invoke(runtimeConf, SESSION_CREDENTIAL_KEY, sharedCredential);
      if (StringUtils.isNotBlank(sessionCredential)) {
        return sessionCredential;
      }
    } catch (Throwable e) {
      // No active SparkSession (e.g. non-Spark GVFS usage). sharedCredential may still be null —
      // getAccessToken() will throw SecurityException in that case to avoid silent admin fallback.
    }
    return sharedCredential;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder
      extends OAuth2TokenProviderBuilder<Builder, GvfsSessionAwareOAuth2TokenProvider> {

    private String credential;
    private String scope;
    private String path;

    public Builder withCredential(String credential) {
      this.credential = credential;
      return this;
    }

    public Builder withScope(String scope) {
      this.scope = scope;
      return this;
    }

    public Builder withPath(String path) {
      this.path = path;
      return this;
    }

    @Override
    protected GvfsSessionAwareOAuth2TokenProvider internalBuild() {
      // credential intentionally optional — session-oauth2 relies on per-user credential from
      // the active SparkSession at token-fetch time; no bootstrap token is pre-fetched, and
      // no admin fallback is wired. getAccessToken() fails fast if nothing resolves.
      Preconditions.checkArgument(
          StringUtils.isNotBlank(scope), "OAuth2TokenProvider must set scope");
      Preconditions.checkArgument(
          StringUtils.isNotBlank(path), "OAuth2TokenProvider must set path");

      GvfsSessionAwareOAuth2TokenProvider provider = new GvfsSessionAwareOAuth2TokenProvider();
      provider.client = client;
      provider.sharedCredential = credential;
      provider.scope = scope;
      provider.path = path;
      return provider;
    }
  }
}
