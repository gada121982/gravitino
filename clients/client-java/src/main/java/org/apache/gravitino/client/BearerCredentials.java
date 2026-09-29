/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.client;

import java.util.regex.Pattern;

/**
 * Tells a bearer access token apart from an OAuth2 client credential.
 *
 * <p>Dataprise compute authenticates to Gravitino with the user's own access token (an IAM token,
 * or a short-lived Keycloak token for a run-as job) instead of a client credential that the engine
 * exchanges for a token. Both reach the session-aware token providers through the same key ({@code
 * spark.sql.gravitino.oauth2.credential}), so the providers use this to decide whether to send the
 * value as-is or to run the client-credentials exchange.
 *
 * <p>A bearer token here is a compact JWS: three base64url segments separated by dots, optionally
 * prefixed with {@code "Bearer "}. A client credential is {@code clientId:clientSecret} and never
 * matches, because ':' is not a base64url character.
 */
public final class BearerCredentials {

  private static final String BEARER_PREFIX = "Bearer ";
  private static final Pattern COMPACT_JWS =
      Pattern.compile("^[A-Za-z0-9_-]+\\.[A-Za-z0-9_-]+\\.[A-Za-z0-9_-]+$");

  private BearerCredentials() {}

  /**
   * @param credential the configured credential, may be null
   * @return true when the credential is a bearer access token to be sent as-is
   */
  public static boolean isBearerToken(String credential) {
    return credential != null && COMPACT_JWS.matcher(strip(credential)).matches();
  }

  /**
   * @param credential a value for which {@link #isBearerToken(String)} is true
   * @return the token without an optional "Bearer " prefix and surrounding whitespace
   */
  public static String token(String credential) {
    return strip(credential);
  }

  private static String strip(String credential) {
    String c = credential.trim();
    if (c.regionMatches(true, 0, BEARER_PREFIX, 0, BEARER_PREFIX.length())) {
      c = c.substring(BEARER_PREFIX.length()).trim();
    }
    return c;
  }
}
