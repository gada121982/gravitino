/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.iceberg.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Tests the prefix-scoped signer route used when the remote catalog cannot load a table that is not
 * committed yet, which is the case for a staged create (CTAS).
 */
public class TestFederatedCatalogWrapper {

  @Test
  void testPrefixScopedSignPath() {
    Map<String, String> properties =
        ImmutableMap.of("prefix", "8f8a1e9c-2c1a-4a3e-9b0e-8b0a5f4a1c2d");

    assertEquals(
        "v1/8f8a1e9c-2c1a-4a3e-9b0e-8b0a5f4a1c2d/v1/aws/s3/sign",
        FederatedCatalogWrapper.prefixScopedSignPath(properties));
  }

  @Test
  void testPrefixScopedSignPathEncodesPrefix() {
    Map<String, String> properties = ImmutableMap.of("prefix", "ware house/1");

    assertEquals(
        "v1/ware+house%2F1/v1/aws/s3/sign",
        FederatedCatalogWrapper.prefixScopedSignPath(properties));
  }

  @Test
  void testPrefixScopedSignPathWithoutPrefix() {
    // A remote catalog that is not addressed under a prefix has no such route, so the caller must
    // keep the original error rather than proxy to a path that cannot exist.
    assertNull(FederatedCatalogWrapper.prefixScopedSignPath(Collections.emptyMap()));

    Map<String, String> blankPrefix = new HashMap<>();
    blankPrefix.put("prefix", "  ");
    assertNull(FederatedCatalogWrapper.prefixScopedSignPath(blankPrefix));
  }
}
