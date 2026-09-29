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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Runs without an active SparkSession, so the provider falls back to the shared credential. No
 * request reaches the (unreachable) token endpoint: a bearer token is returned as-is, and a missing
 * credential is refused before any exchange is attempted.
 */
public class TestSessionAwareOAuth2TokenProvider {

  private static final String JWT = "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ1In0.c2ln";

  private static SessionAwareOAuth2TokenProvider.Builder builder() {
    return SessionAwareOAuth2TokenProvider.builder()
        .withUri("http://127.0.0.1:1")
        .withPath("realms/r/protocol/openid-connect/token")
        .withScope("gravitino");
  }

  @Test
  void testSharedBearerTokenIsSentAsIs() throws Exception {
    try (SessionAwareOAuth2TokenProvider provider = builder().withCredential(JWT).build()) {
      Assertions.assertEquals(JWT, provider.getAccessToken());
    }
  }

  @Test
  void testNoCredentialIsRefused() throws Exception {
    try (SessionAwareOAuth2TokenProvider provider = builder().build()) {
      Assertions.assertThrows(SecurityException.class, provider::getAccessToken);
    }
  }
}
