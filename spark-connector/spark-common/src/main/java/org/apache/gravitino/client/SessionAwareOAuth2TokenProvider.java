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
import org.apache.spark.sql.SparkSession;

/**
 * Session-aware OAuth2 token provider that resolves per-user credentials from the active
 * SparkSession's SQLConf. Falls back to the shared credential when no active session exists or no
 * per-user credential is configured.
 *
 * <p>This enables multi-tenant identity: CatalogSyncExtension sets
 * "spark.sql.gravitino.oauth2.credential" per-session, and all Gravitino API calls within that
 * session automatically use the user's own OAuth2 identity.
 */
public class SessionAwareOAuth2TokenProvider extends OAuth2TokenProvider {

  private String sharedCredential;
  private String scope;
  private String path;

  private final ConcurrentHashMap<String, String> tokenCache = new ConcurrentHashMap<>();

  private SessionAwareOAuth2TokenProvider() {}

  @Override
  protected synchronized String getAccessToken() {
    String credential = resolveCredential();
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
      SparkSession session = SparkSession.active();
      String sessionCredential =
          session.conf().get("spark.sql.gravitino.oauth2.credential", sharedCredential);
      if (StringUtils.isNotBlank(sessionCredential)) {
        return sessionCredential;
      }
    } catch (Exception e) {
      // No active session — fall back to shared credential
    }
    return sharedCredential;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder
      extends OAuth2TokenProviderBuilder<Builder, SessionAwareOAuth2TokenProvider> {

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
    protected SessionAwareOAuth2TokenProvider internalBuild() {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(credential), "OAuth2TokenProvider must set credential");
      Preconditions.checkArgument(
          StringUtils.isNotBlank(scope), "OAuth2TokenProvider must set scope");
      Preconditions.checkArgument(
          StringUtils.isNotBlank(path), "OAuth2TokenProvider must set path");

      SessionAwareOAuth2TokenProvider provider = new SessionAwareOAuth2TokenProvider();
      provider.client = client;
      provider.sharedCredential = credential;
      provider.scope = scope;
      provider.path = path;

      // Fetch initial token with shared credential
      provider.tokenCache.put(
          credential,
          OAuth2ClientUtil.fetchToken(client, Collections.emptyMap(), credential, scope, path)
              .getAccessToken());
      return provider;
    }
  }
}
