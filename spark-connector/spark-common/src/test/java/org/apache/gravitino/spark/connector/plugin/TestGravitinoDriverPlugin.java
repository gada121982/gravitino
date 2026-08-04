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

import java.util.Map;
import org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestGravitinoDriverPlugin {

  @Test
  void testIcebergExtensionName() {
    Assertions.assertEquals(
        IcebergSparkSessionExtensions.class.getName(),
        GravitinoDriverPlugin.ICEBERG_SPARK_EXTENSIONS);
  }

  @Test
  void testIcebergRestCatalogConf() {
    Map<String, String> conf =
        GravitinoDriverPlugin.icebergRestCatalogConf(
            "sales",
            "lake",
            "https://rest.example.com/iceberg/",
            "client:secret",
            "https://idp.example.com",
            "realms/r/protocol/openid-connect/token",
            "gravitino");

    Assertions.assertEquals(
        "org.apache.iceberg.spark.SparkCatalog", conf.get("spark.sql.catalog.sales"));
    Assertions.assertEquals("rest", conf.get("spark.sql.catalog.sales.type"));
    Assertions.assertEquals(
        "https://rest.example.com/iceberg/", conf.get("spark.sql.catalog.sales.uri"));
    Assertions.assertEquals("lake.sales", conf.get("spark.sql.catalog.sales.warehouse"));
    Assertions.assertEquals(
        "remote-signing", conf.get("spark.sql.catalog.sales.header.X-Iceberg-Access-Delegation"));
    Assertions.assertEquals("client:secret", conf.get("spark.sql.catalog.sales.credential"));
    Assertions.assertEquals("gravitino", conf.get("spark.sql.catalog.sales.scope"));
  }

  @Test
  void testIcebergRestCatalogConfJoinsTokenPathWithoutDoubleSlash() {
    Map<String, String> conf =
        GravitinoDriverPlugin.icebergRestCatalogConf(
            "sales",
            "lake",
            "https://rest.example.com/iceberg/",
            "client:secret",
            "https://idp.example.com/",
            "/realms/r/protocol/openid-connect/token",
            "gravitino");

    Assertions.assertEquals(
        "https://idp.example.com/realms/r/protocol/openid-connect/token",
        conf.get("spark.sql.catalog.sales.oauth2-server-uri"));
  }

  @Test
  void testIcebergRestCatalogConfOmitsBlankOptionalEntries() {
    Map<String, String> conf =
        GravitinoDriverPlugin.icebergRestCatalogConf(
            "sales", "lake", "https://rest.example.com/iceberg/", "", "", "", "");

    Assertions.assertFalse(conf.containsKey("spark.sql.catalog.sales.credential"));
    Assertions.assertFalse(conf.containsKey("spark.sql.catalog.sales.oauth2-server-uri"));
    Assertions.assertFalse(conf.containsKey("spark.sql.catalog.sales.scope"));
    // The entries that make the catalog resolvable at all must always be present.
    Assertions.assertEquals("rest", conf.get("spark.sql.catalog.sales.type"));
    Assertions.assertEquals("lake.sales", conf.get("spark.sql.catalog.sales.warehouse"));
  }
}
