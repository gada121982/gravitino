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

package org.apache.gravitino.server.web.filter;

import java.lang.reflect.Parameter;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.iceberg.common.ops.IcebergCatalogWrapper;
import org.apache.gravitino.iceberg.service.IcebergRESTUtils;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.authorization.annotations.AuthorizationMetadata;
import org.apache.gravitino.server.authorization.annotations.IcebergAuthorizationMetadata;
import org.apache.gravitino.server.authorization.annotations.IcebergAuthorizationMetadata.RequestType;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionEvaluator;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.rest.RESTUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Shared helper for Iceberg load table/view authorization handlers. */
final class IcebergLoadAuthzHandlerHelper {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergLoadAuthzHandlerHelper.class);

  private IcebergLoadAuthzHandlerHelper() {}

  static LoadTarget extractLoadTarget(
      Parameter[] parameters, Object[] args, RequestType requestType) {
    String targetName = null;
    Namespace namespace = null;

    for (int i = 0; i < parameters.length; i++) {
      Parameter parameter = parameters[i];

      IcebergAuthorizationMetadata icebergMetadata =
          parameter.getAnnotation(IcebergAuthorizationMetadata.class);
      if (icebergMetadata != null && icebergMetadata.type() == requestType) {
        targetName = RESTUtil.decodeString(String.valueOf(args[i]));
      }

      AuthorizationMetadata authMetadata = parameter.getAnnotation(AuthorizationMetadata.class);
      if (authMetadata != null && authMetadata.type() == EntityType.SCHEMA) {
        namespace =
            RESTUtil.decodeNamespace(
                String.valueOf(args[i]), IcebergRESTUtils.NAMESPACE_SEPARATOR_URLENCODED_UTF_8);
      }
    }

    return new LoadTarget(targetName, namespace);
  }

  static LoadContext extractLoadContext(
      Map<EntityType, NameIdentifier> nameIdentifierMap,
      Supplier<RuntimeException> missingContextException) {
    NameIdentifier catalogId = nameIdentifierMap.get(EntityType.CATALOG);
    NameIdentifier schemaId = nameIdentifierMap.get(EntityType.SCHEMA);

    if (catalogId == null || schemaId == null) {
      throw missingContextException.get();
    }

    return new LoadContext(catalogId.namespace().level(0), catalogId.name(), schemaId.name());
  }

  static IcebergCatalogWrapper getCatalogWrapper(String catalog) {
    return IcebergRESTServerContext.getInstance()
        .catalogWrapperManager()
        .getCatalogWrapper(catalog);
  }

  static String resolveExpression(
      AuthorizationExpression authorizationExpression, String defaultExpression) {
    if (authorizationExpression != null
        && authorizationExpression.expression() != null
        && !authorizationExpression.expression().isBlank()) {
      return authorizationExpression.expression();
    }
    return defaultExpression;
  }

  static String resolveAllowCheckExistenceExpression(
      AuthorizationExpression authorizationExpression, String defaultExpression) {
    if (authorizationExpression != null) {
      String expr = authorizationExpression.allowCheckExistence();
      if (expr != null && !expr.isBlank()) {
        return expr;
      }
    }
    return defaultExpression;
  }

  static void authorizeLoadEntity(
      Map<EntityType, NameIdentifier> nameIdentifierMap,
      String primaryExpression,
      String allowCheckExistenceExpression,
      BooleanSupplier exists,
      Supplier<RuntimeException> notFoundException,
      String entityType,
      NameIdentifier entityId) {
    Map<String, Object> emptyPathParams = Collections.emptyMap();
    AuthorizationRequestContext requestContext = new AuthorizationRequestContext();
    Optional<String> emptyEntityType = Optional.empty();

    // An identifier that cannot be expressed as a metadata object is not something this
    // catalog governs, so there is no privilege to evaluate against it. Spark's datasource
    // shortcut — SELECT * FROM csv.`gvfs://…/part-00000-….csv` — resolves as a table whose
    // name is a file path, and MetadataObjects.parse re-splits "catalog.schema.<name>" on
    // dots, so any dot in the path yields more than three parts and throws. Left
    // unhandled that surfaced as "Authorization failed due to system internal error" with
    // HTTP 500, telling the user nothing and hiding a path problem behind an authz one.
    //
    // Report it as not-found instead. That is both accurate (no such table exists) and
    // useful: Spark falls back to reading the path directly. Note this is reached only by
    // principals the earlier authorization did not already clear — an owner short-circuits
    // before here, which is why the failure looked intermittent and identity-specific.
    try {
      AuthorizationExpressionEvaluator primaryEvaluator =
          new AuthorizationExpressionEvaluator(primaryExpression);
      if (primaryEvaluator.evaluate(
          nameIdentifierMap, emptyPathParams, requestContext, emptyEntityType)) {
        return;
      }

      AuthorizationExpressionEvaluator allowExistenceEvaluator =
          new AuthorizationExpressionEvaluator(allowCheckExistenceExpression);
      if (allowExistenceEvaluator.evaluate(
              nameIdentifierMap, emptyPathParams, requestContext, emptyEntityType)
          && !exists.getAsBoolean()) {
        throw notFoundException.get();
      }
    } catch (IllegalArgumentException e) {
      LOG.debug(
          "Cannot authorize {} '{}': the identifier is not a valid metadata object, "
              + "reporting it as not found",
          entityType,
          entityId,
          e);
      throw notFoundException.get();
    }

    String currentUser = PrincipalUtils.getCurrentUserName();
    throw new ForbiddenException(
        "User '%s' is not authorized to load %s '%s'", currentUser, entityType, entityId);
  }

  static final class LoadTarget {
    private final String name;
    private final Namespace namespace;

    private LoadTarget(String name, Namespace namespace) {
      this.name = name;
      this.namespace = namespace;
    }

    boolean hasMissingPart() {
      return name == null || namespace == null;
    }

    String name() {
      return name;
    }

    Namespace namespace() {
      return namespace;
    }
  }

  static final class LoadContext {
    private final String metalakeName;
    private final String catalog;
    private final String schema;

    private LoadContext(String metalakeName, String catalog, String schema) {
      this.metalakeName = metalakeName;
      this.catalog = catalog;
      this.schema = schema;
    }

    String metalakeName() {
      return metalakeName;
    }

    String catalog() {
      return catalog;
    }

    String schema() {
      return schema;
    }
  }
}
