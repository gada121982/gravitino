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

public class TestBearerCredentials {

  private static final String JWT = "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ1In0.c2ln-_x";

  @Test
  void testRecognisesCompactJws() {
    Assertions.assertTrue(BearerCredentials.isBearerToken(JWT));
    Assertions.assertTrue(BearerCredentials.isBearerToken("Bearer " + JWT));
    Assertions.assertTrue(BearerCredentials.isBearerToken("  bearer " + JWT + "  "));
    Assertions.assertEquals(JWT, BearerCredentials.token("Bearer " + JWT));
  }

  @Test
  void testClientCredentialIsNotABearerToken() {
    Assertions.assertFalse(BearerCredentials.isBearerToken("client-id:client-secret"));
    // A client id that contains dots is still a client credential because of the ':'.
    Assertions.assertFalse(BearerCredentials.isBearerToken("a.b.c:secret"));
    Assertions.assertFalse(BearerCredentials.isBearerToken(null));
    Assertions.assertFalse(BearerCredentials.isBearerToken(""));
    Assertions.assertFalse(BearerCredentials.isBearerToken("a.b"));
    Assertions.assertFalse(BearerCredentials.isBearerToken("a..c"));
  }
}
