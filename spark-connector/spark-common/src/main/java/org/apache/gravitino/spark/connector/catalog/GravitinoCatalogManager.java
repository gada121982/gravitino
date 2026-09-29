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
package org.apache.gravitino.spark.connector.catalog;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import java.util.Arrays;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.client.GravitinoClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** GravitinoCatalogManager is used to retrieve catalogs from Apache Gravitino server. */
public class GravitinoCatalogManager {
  private static final Logger LOG = LoggerFactory.getLogger(GravitinoCatalogManager.class);
  private static GravitinoCatalogManager gravitinoCatalogManager;

  private volatile boolean isClosed = false;
  private volatile boolean clientBuilt = false;
  private final Cache<String, Catalog> gravitinoCatalogs;

  // Built on first use, not at driver start. Building a GravitinoClient loads the metalake, which
  // needs a credential. Engines that serve users (Spark Connect, SQL warehouse) start with none:
  // the first call happens inside a user session, whose own token the session-aware provider
  // resolves. Batch jobs carry their run-as token from the start, so for them nothing changes.
  private final Supplier<GravitinoClient> gravitinoClient;

  private GravitinoCatalogManager(Supplier<GravitinoClient> clientBuilder) {
    this.gravitinoClient =
        Suppliers.memoize(
            () -> {
              GravitinoClient client = clientBuilder.get();
              clientBuilt = true;
              return client;
            });
    // Will not evict catalog by default
    this.gravitinoCatalogs = Caffeine.newBuilder().build();
  }

  public static GravitinoCatalogManager create(Supplier<GravitinoClient> clientBuilder) {
    Preconditions.checkState(
        gravitinoCatalogManager == null, "Should not create duplicate GravitinoCatalogManager");
    gravitinoCatalogManager = new GravitinoCatalogManager(clientBuilder);
    return gravitinoCatalogManager;
  }

  public static GravitinoCatalogManager get() {
    Preconditions.checkState(
        gravitinoCatalogManager != null, "GravitinoCatalogManager has not created yet");
    Preconditions.checkState(
        !gravitinoCatalogManager.isClosed, "GravitinoCatalogManager is already closed");
    return gravitinoCatalogManager;
  }

  public void close() {
    Preconditions.checkState(!isClosed, "Gravitino Catalog is already closed");
    isClosed = true;
    if (clientBuilt) {
      gravitinoClient.get().close();
    }
    gravitinoCatalogManager = null;
  }

  public Catalog getGravitinoCatalogInfo(String name) {
    try {
      return gravitinoCatalogs.get(name, catalogName -> loadCatalog(catalogName));
    } catch (Exception e) {
      LOG.error(String.format("Load catalog %s failed", name), e);
      throw new RuntimeException(e);
    }
  }

  public void loadRelationalCatalogs() {
    Catalog[] catalogs = gravitinoClient.get().listCatalogsInfo();
    Arrays.stream(catalogs)
        .filter(catalog -> Catalog.Type.RELATIONAL.equals(catalog.type()))
        .forEach(catalog -> gravitinoCatalogs.put(catalog.name(), catalog));
  }

  public Map<String, Catalog> getCatalogs() {
    return gravitinoCatalogs.asMap();
  }

  private Catalog loadCatalog(String catalogName) {
    Catalog catalog = gravitinoClient.get().loadCatalog(catalogName);
    Preconditions.checkArgument(
        Catalog.Type.RELATIONAL.equals(catalog.type()), "Only support relational catalog");
    LOG.info("Load catalog {} from Gravitino successfully.", catalogName);
    return catalog;
  }
}
