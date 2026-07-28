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

package org.apache.gravitino.listener.api.event;

import com.google.common.collect.ImmutableMap;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.iceberg.service.IcebergAccessDelegation;
import org.apache.gravitino.iceberg.service.IcebergRESTUtils;
import org.apache.gravitino.server.web.ClientAddressResolver;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.gravitino.utils.RequestContext;

/**
 * The general request context information for Iceberg REST operations.
 *
 * <p>Optional {@link #auditExtras()} are facts an inner dispatcher wants on the terminal Iceberg
 * event. They are not HTTP headers and never appear in {@link #httpHeaders()}.
 */
public class IcebergRequestContext {

  /** Header that opts a drop purge request into asynchronous file cleanup. */
  public static final String ASYNC_PURGE_HEADER = "X-Gravitino-Async-Purge";

  private static final String UNKNOWN_ADDRESS = "unknown";

  /**
   * @deprecated Kept only for backward-compatibility and will be removed in the next major release.
   */
  @Deprecated private final HttpServletRequest httpServletRequest;

  private final String catalogName;
  private final String metalakeName;
  private final String simpleCatalogName;
  private final String userName;
  private final String remoteHostName;
  private final Map<String, String> httpHeaders;
  private final IcebergAccessDelegation accessDelegation;
  private final Map<String, String> auditExtras;

  /**
   * Constructs a new {@code IcebergRequestContext} instance.
   *
   * @param httpRequest The HttpServletRequest object containing request details.
   * @param catalogName The name of the catalog to be accessed in the request.
   */
  public IcebergRequestContext(HttpServletRequest httpRequest, String catalogName) {
    this(httpRequest, catalogName, IcebergAccessDelegation.none());
  }

  /**
   * Constructs a new {@code IcebergRequestContext} instance.
   *
   * @param httpRequest The HttpServletRequest object containing request details.
   * @param catalogName The name of the catalog to be accessed in the request.
   * @param accessDelegation parsed access delegation header
   */
  public IcebergRequestContext(
      HttpServletRequest httpRequest,
      String catalogName,
      IcebergAccessDelegation accessDelegation) {
    this(
        httpRequest,
        catalogName,
        PrincipalUtils.getCurrentUserName(),
        resolveClientAddress(httpRequest),
        IcebergRESTUtils.getHttpHeaders(httpRequest),
        accessDelegation,
        ImmutableMap.of());
  }

  /**
   * Constructs a new {@code IcebergRequestContext} instance.
   *
   * @param httpRequest The HttpServletRequest object containing request details.
   * @param catalogName The name of the catalog to be accessed in the request.
   * @param requestCredentialVending whether the client asked for vended credentials
   */
  public IcebergRequestContext(
      HttpServletRequest httpRequest, String catalogName, boolean requestCredentialVending) {
    this(httpRequest, catalogName, IcebergAccessDelegation.of(requestCredentialVending, false));
  }

  private IcebergRequestContext(
      HttpServletRequest httpServletRequest,
      String catalogName,
      String userName,
      String remoteHostName,
      Map<String, String> httpHeaders,
      IcebergAccessDelegation accessDelegation,
      Map<String, String> auditExtras) {
    this.httpServletRequest = httpServletRequest;
    this.catalogName = catalogName;
    // catalogName is the raw Iceberg REST prefix, which may be metalake-qualified
    // ("{metalake}.{catalog}"). Resolve the addressed pair once here so every consumer that builds
    // a Gravitino NameIdentifier uses the request's metalake instead of the server-configured one.
    // Building an identifier from (configured metalake, raw prefix) yields a doubled metalake such
    // as "ml.ml.catalog" and fails catalog lookup, so prefer metalake()/simpleCatalogName().
    IcebergRESTUtils.MetalakeCatalog metalakeCatalog =
        IcebergRESTUtils.parseMetalakeCatalog(catalogName);
    this.metalakeName = metalakeCatalog.metalake();
    this.simpleCatalogName = metalakeCatalog.catalog();
    this.userName = userName;
    this.remoteHostName = remoteHostName;
    this.httpHeaders = httpHeaders;
    this.accessDelegation = accessDelegation;
    this.auditExtras = auditExtras;
  }

  private static String resolveClientAddress(HttpServletRequest request) {
    // RequestContextFilter, installed ahead of every Iceberg REST endpoint, has already resolved
    // the client address with the server's trusted-proxy rules. Without it, use the TCP peer
    // address, in the same canonical form: X-Forwarded-For cannot be trusted here because the
    // trusted proxies are unknown. A missing address is recorded as "unknown", as other events do.
    String contextAddress = RequestContext.getRemoteAddress();
    String address =
        contextAddress != null
            ? contextAddress
            : ClientAddressResolver.noTrustedProxies().resolve(request);
    return StringUtils.isBlank(address) ? UNKNOWN_ADDRESS : address;
  }

  /**
   * Returns the catalog name.
   *
   * @return The catalog name.
   */
  public String catalogName() {
    return catalogName;
  }

  /**
   * Returns the metalake addressed by this request, resolved from the Iceberg REST prefix. Falls
   * back to the server-configured metalake when the prefix is not metalake-qualified.
   *
   * @return The metalake name.
   */
  public String metalakeName() {
    return metalakeName;
  }

  /**
   * Returns the catalog name without the metalake qualifier. Use this (with {@link
   * #metalakeName()}) when building Gravitino identifiers; {@link #catalogName()} returns the raw
   * prefix and is only appropriate for selecting the Iceberg catalog wrapper.
   *
   * @return The catalog name.
   */
  public String simpleCatalogName() {
    return simpleCatalogName;
  }

  /**
   * Returns the username of the HTTP client.
   *
   * @return The username.
   */
  public String userName() {
    return userName;
  }

  /**
   * Returns the address of the HTTP client, resolved with the server's trusted-proxy rules (see
   * {@code trustedProxies} in the Jetty server configuration). Despite the method name, this is an
   * IP address, never a resolved host name.
   *
   * @return The remote client address.
   */
  public String remoteHostName() {
    return remoteHostName;
  }

  /**
   * Returns the Map of the HTTP headers.
   *
   * @return The HTTP header.
   */
  public Map<String, String> httpHeaders() {
    return httpHeaders;
  }

  /**
   * Returns the immutable audit extras attached to this context. Empty when none were attached.
   *
   * @return audit extras, never {@code null}
   */
  public Map<String, String> auditExtras() {
    return auditExtras;
  }

  /**
   * Returns a copy of this context with the given audit extras. Extras are not part of {@link
   * #httpHeaders()}. A {@code null} or empty map yields a context with empty extras.
   *
   * @param extras optional facts for {@code customInfo()}, or {@code null} for none
   * @return a new context; this instance is unchanged
   */
  public IcebergRequestContext withAuditExtras(Map<String, String> extras) {
    return new IcebergRequestContext(
        httpServletRequest,
        catalogName,
        userName,
        remoteHostName,
        httpHeaders,
        accessDelegation,
        copyAuditExtras(extras));
  }

  /**
   * Returns HTTP headers merged with {@link #auditExtras()}. When extras are empty, returns {@link
   * #httpHeaders()} unchanged so existing callers keep today's header-only map. Extra keys overlay
   * headers on conflict.
   *
   * @return headers, or headers overlaid with extras
   */
  public Map<String, String> customInfo() {
    if (auditExtras.isEmpty()) {
      return httpHeaders;
    }
    Map<String, String> merged = new LinkedHashMap<>(httpHeaders);
    merged.putAll(auditExtras);
    return ImmutableMap.copyOf(merged);
  }

  /**
   * Returns parsed {@code X-Iceberg-Access-Delegation} capabilities requested by the client.
   *
   * @return access delegation mode
   */
  public IcebergAccessDelegation accessDelegation() {
    return accessDelegation;
  }

  /**
   * Returns whether the client asked for vended credentials.
   *
   * @return true when {@code vended-credentials} was requested
   */
  public boolean requestCredentialVending() {
    return accessDelegation.requestVendedCredentials();
  }

  /**
   * Checks whether this request opted into asynchronous table purge.
   *
   * <p>Async purge is opt-in. Standard Iceberg clients send no header and keep synchronous purge
   * behavior; a client opts in with {@code X-Gravitino-Async-Purge: true}.
   *
   * @return true only when the async purge header explicitly says {@code true}
   */
  public boolean asyncPurge() {
    for (Map.Entry<String, String> header : httpHeaders.entrySet()) {
      // HTTP header names are case-insensitive; the value is matched exactly as "true".
      if (ASYNC_PURGE_HEADER.equalsIgnoreCase(header.getKey())) {
        return "true".equals(header.getValue().trim());
      }
    }
    return false;
  }

  /**
   * Retrieves the HttpServletRequest object. This method is deprecated and should be used
   * cautiously.
   *
   * @return The HttpServletRequest associated with this context.
   * @deprecated Use other methods to access specific request details instead.
   */
  @Deprecated
  public HttpServletRequest getHttpServletRequest() {
    return httpServletRequest;
  }

  private static Map<String, String> copyAuditExtras(Map<String, String> extras) {
    if (extras == null || extras.isEmpty()) {
      return ImmutableMap.of();
    }
    return ImmutableMap.copyOf(extras);
  }
}
