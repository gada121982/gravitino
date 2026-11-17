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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.rest.credentials.Credential;
import org.apache.iceberg.rest.credentials.ImmutableCredential;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/**
 * Tests the federated wrapper's static helpers: the prefix-scoped signer route used when the remote
 * catalog cannot load a table that is not committed yet (a staged create, CTAS), the removal of the
 * remote catalog's signer config from remote table responses, and when this server's signer config
 * is added instead.
 */
public class TestFederatedCatalogWrapper {

  private static final String LOCATION = "s3://bucket/db/tbl";

  private static TableMetadata tableMetadata() {
    return TableMetadataParser.fromJson(
        LOCATION + "/metadata/v1.metadata.json",
        TableMetadataParser.toJson(
            TableMetadata.newTableMetadata(
                new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get())),
                PartitionSpec.unpartitioned(),
                SortOrder.unsorted(),
                LOCATION,
                Collections.emptyMap())));
  }

  @Test
  void testRemoteSignerConfigIsRemoved() {
    // What Lakekeeper returns for a remote-signing table: its own signer URI (the in-cluster base
    // URI) and per-table signer route, under both the S3-namespaced and the generic keys.
    Map<String, String> upstreamConfig = new HashMap<>();
    upstreamConfig.put("s3.remote-signing-enabled", "true");
    upstreamConfig.put("s3.signer", "S3V4RestSigner");
    upstreamConfig.put(
        "s3.signer.uri", "http://lakekeeper.lakekeeper.svc.cluster.local:8181/catalog/");
    upstreamConfig.put("s3.signer.endpoint", "v1/signer/wh/tabular-id/t/v1/aws/s3/sign");
    upstreamConfig.put(
        "signer.uri", "http://lakekeeper.lakekeeper.svc.cluster.local:8181/catalog/");
    upstreamConfig.put("signer.endpoint", "v1/signer/wh/tabular-id/t/v1/aws/s3/sign");
    upstreamConfig.put("s3.region", "HCM03");
    upstreamConfig.put("s3.endpoint", "https://hcm03.vstorage.vngcloud.vn");
    upstreamConfig.put("s3.path-style-access", "true");
    TableMetadata metadata = tableMetadata();
    List<Credential> credentials =
        Collections.singletonList(
            ImmutableCredential.builder()
                .prefix(LOCATION)
                .config(ImmutableMap.of("s3.access-key-id", "key"))
                .build());
    LoadTableResponse upstream =
        LoadTableResponse.builder()
            .withTableMetadata(metadata)
            .addAllConfig(upstreamConfig)
            .addAllCredentials(credentials)
            .build();

    LoadTableResponse cleaned = FederatedCatalogWrapper.withoutRemoteSignerConfig(upstream);

    assertEquals(
        ImmutableMap.of(
            "s3.region", "HCM03",
            "s3.endpoint", "https://hcm03.vstorage.vngcloud.vn",
            "s3.path-style-access", "true"),
        cleaned.config());
    assertEquals(credentials, cleaned.credentials());
    assertEquals(metadata.metadataFileLocation(), cleaned.metadataLocation());
    assertEquals(LOCATION, cleaned.tableMetadata().location());
  }

  @Test
  void testRemoteSignAppliesWithoutLocalCredentialProvider() {
    // The federated catalog configures no credential provider of its own (signing is proxied to the
    // remote catalog), so remote signing must not depend on one.
    assertTrue(
        FederatedCatalogWrapper.remoteSignApplies(
            LOCATION, IcebergAccessDelegation.of(false, true)));
    assertTrue(
        FederatedCatalogWrapper.remoteSignApplies(
            LOCATION, IcebergAccessDelegation.of(true, true)));
  }

  @Test
  void testRemoteSignNotAppliedWhenNotRequested() {
    assertFalse(
        FederatedCatalogWrapper.remoteSignApplies(
            LOCATION, IcebergAccessDelegation.of(true, false)));
    assertFalse(
        FederatedCatalogWrapper.remoteSignApplies(LOCATION, IcebergAccessDelegation.none()));
  }

  @Test
  void testRemoteSignNotAppliedToLocalOrHdfsTables() {
    IcebergAccessDelegation remoteSigning = IcebergAccessDelegation.of(false, true);
    assertFalse(FederatedCatalogWrapper.remoteSignApplies("hdfs://nn/db/tbl", remoteSigning));
    assertFalse(FederatedCatalogWrapper.remoteSignApplies("file:///tmp/db/tbl", remoteSigning));
    assertThrows(
        IllegalArgumentException.class,
        () -> FederatedCatalogWrapper.remoteSignApplies(" ", remoteSigning));
  }

  @Test
  void testResponseWithoutRemoteSignerConfigIsUnchanged() {
    LoadTableResponse upstream =
        LoadTableResponse.builder()
            .withTableMetadata(tableMetadata())
            .addAllConfig(ImmutableMap.of("s3.region", "HCM03"))
            .build();

    assertSame(upstream, FederatedCatalogWrapper.withoutRemoteSignerConfig(upstream));
  }

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
