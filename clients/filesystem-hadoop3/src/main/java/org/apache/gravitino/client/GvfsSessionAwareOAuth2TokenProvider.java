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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Session-aware OAuth2 token provider for GVFS. Resolves per-user OAuth2 credential with the
 * following precedence:
 *
 * <ol>
 *   <li>Spark {@code TaskContext} local property — works on the executor because Spark serializes
 *       local properties into each {@code TaskDescription}. This is the ONLY channel that
 *       propagates per-user state from driver to executor without leaking into JVM-global Hadoop
 *       conf (which would race between concurrent users in a shared engine).
 *   <li>Active {@code SparkSession} runtime conf — works on the driver, where {@code
 *       CatalogSyncExtension} wrote the credential via {@code session.conf.set}.
 *   <li>Shared credential from builder — optional fallback, typically empty to force fail-fast
 *       instead of silent admin impersonation.
 * </ol>
 *
 * <p>The filesystem-hadoop3 module cannot depend on Spark, so both Spark classes are loaded via
 * reflection. This keeps the provider safe to load in non-Spark environments (plain Hadoop, Flink,
 * Trino, etc.) where only the shared-credential fallback applies.
 *
 * <p>Wiring: set {@code fs.gravitino.client.authType=session-oauth2} in Hadoop/Spark conf; {@link
 * org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystemUtils} will build the
 * GravitinoClient with this provider.
 */
public class GvfsSessionAwareOAuth2TokenProvider extends OAuth2TokenProvider {

  private static final Logger LOG =
      LoggerFactory.getLogger(GvfsSessionAwareOAuth2TokenProvider.class);

  // Config/property key written by CatalogSyncExtension to BOTH SparkSession.conf (driver-side) and
  // sparkContext.setLocalProperty (propagates to executor via TaskContext). Reusing the same key
  // means SQL and GVFS share one per-user credential on both sides of the driver/executor boundary.
  private static final String SESSION_CREDENTIAL_KEY = "spark.sql.gravitino.oauth2.credential";

  private String sharedCredential;
  private String scope;
  private String path;

  // Token cache keyed by credential string — lets users in the same JVM each keep their own token
  // without re-fetching on every call.
  private final ConcurrentHashMap<String, String> tokenCache = new ConcurrentHashMap<>();

  private GvfsSessionAwareOAuth2TokenProvider() {}

  @Override
  protected synchronized String getAccessToken() {
    String credential = resolveCredential();
    if (StringUtils.isBlank(credential)) {
      // Fail-fast: no per-user credential in SparkSession and no shared fallback configured.
      // Returning null would make Gravitino silently use admin identity — refuse instead so
      // RBAC cannot be bypassed when session wiring is broken or called from non-Spark code.
      LOG.warn(
          "[thread={}] no credential from TaskContext, CREDENTIAL_STORE, or SparkSession — throwing SecurityException",
          Thread.currentThread().getName());
      throw new SecurityException(
          "GvfsSessionAwareOAuth2TokenProvider: no per-user OAuth2 credential available "
              + "(spark.sql.gravitino.oauth2.credential not set in active SparkSession and no "
              + "shared fallback configured). Refusing to vend admin token.");
    }
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
    // Executor path: TaskContext.getLocalProperty is how Spark propagates per-job state from
    // driver to executor (see SparkContext.setLocalProperty). On executor SparkSession.active()
    // returns a driver-less stub with no runtime conf, so this is the only reliable channel.
    String taskContextCredential = resolveFromTaskContext();
    if (StringUtils.isNotBlank(taskContextCredential)) {
      LOG.info("[thread={}] resolved via TaskContext", Thread.currentThread().getName());
      return taskContextCredential;
    }

    // Spark Connect driver path: the auth interceptor populates a session-keyed store on the
    // first gRPC message. We consult it BEFORE SparkSession.conf because SparkConf holds an
    // admin-level bootstrap credential needed by GravitinoDriverPlugin at SparkContext init —
    // reading session.conf first would therefore always return the admin fallback even when a
    // per-user credential has already been resolved for this session. This path is also the
    // only way to get the per-user identity for DataFrame / RDD / raw FS operations that never
    // trigger CatalogSyncExtension.
    String interceptorCredential = resolveFromSparkConnectStore();
    if (StringUtils.isNotBlank(interceptorCredential)) {
      LOG.info(
          "[thread={}] resolved via SparkConnect CREDENTIAL_STORE",
          Thread.currentThread().getName());
      // Spark propagates local properties to executor TaskContext at job submission. The
      // CatalogSyncExtension normally sets this during SQL parse, but pure DataFrame reads/writes
      // (spark.read.parquet, df.write.parquet) never trigger the parser. Mirroring here — on the
      // same driver thread that's about to submit the job — is the only way executor tasks get
      // the credential without relying on the extension firing first.
      publishToSparkContext(interceptorCredential);
      return interceptorCredential;
    }

    // Driver path for Kyuubi (IdentitySessionConfAdvisor overlays the credential at session open,
    // overriding the admin default) and SQL queries in Spark Connect (CatalogSyncExtension writes
    // it on first parsePlan). May fall back to the admin bootstrap credential if the per-user
    // resolve hasn't happened yet — this is acceptable at plugin init time but is exactly why
    // the Spark Connect path above takes precedence at runtime.
    String sparkSessionCredential = resolveFromSparkSession();
    if (StringUtils.isNotBlank(sparkSessionCredential)) {
      LOG.info("[thread={}] resolved via SparkSession.conf", Thread.currentThread().getName());
      return sparkSessionCredential;
    }

    // Non-Spark callers or misconfigured sessions: sharedCredential is usually null so that
    // getAccessToken() fails fast instead of silently vending an admin token.
    if (StringUtils.isNotBlank(sharedCredential)) {
      LOG.warn(
          "[thread={}] resolved via sharedCredential admin fallback — RBAC is NOT per-user",
          Thread.currentThread().getName());
    }
    return sharedCredential;
  }

  private void publishToSparkContext(String credential) {
    try {
      Class<?> sparkSessionClass = Class.forName("org.apache.spark.sql.SparkSession");
      Object session = sparkSessionClass.getMethod("active").invoke(null);
      if (session == null) {
        return;
      }
      Object sparkContext = session.getClass().getMethod("sparkContext").invoke(session);
      sparkContext
          .getClass()
          .getMethod("setLocalProperty", String.class, String.class)
          .invoke(sparkContext, SESSION_CREDENTIAL_KEY, credential);
    } catch (Throwable ignore) {
      // Best-effort. If Spark classes are missing or we're on a non-driver thread without an
      // active SparkSession, executors that need this will still throw via resolveFromTaskContext.
    }
  }

  private String resolveFromTaskContext() {
    try {
      Class<?> taskContextClass = Class.forName("org.apache.spark.TaskContext");
      Object taskContext = taskContextClass.getMethod("get").invoke(null);
      if (taskContext == null) {
        // Running on the driver (or in a thread with no TaskContext). Fall through to SparkSession.
        return null;
      }
      return (String)
          taskContext
              .getClass()
              .getMethod("getLocalProperty", String.class)
              .invoke(taskContext, SESSION_CREDENTIAL_KEY);
    } catch (Throwable e) {
      // No Spark on classpath, or incompatible Spark version. Fall through silently so non-Spark
      // environments keep working with sharedCredential.
      return null;
    }
  }

  private String resolveFromSparkSession() {
    try {
      Class<?> sparkSessionClass = Class.forName("org.apache.spark.sql.SparkSession");
      Object session = sparkSessionClass.getMethod("active").invoke(null);
      Object runtimeConf = session.getClass().getMethod("conf").invoke(session);
      return (String)
          runtimeConf
              .getClass()
              .getMethod("get", String.class, String.class)
              .invoke(runtimeConf, SESSION_CREDENTIAL_KEY, sharedCredential);
    } catch (Throwable e) {
      // No active SparkSession (e.g. non-Spark GVFS usage or executor without driver pointer).
      return null;
    }
  }

  /**
   * Read the per-user credential from SparkConnectAuthInterceptor.CREDENTIAL_STORE by discovering
   * the current Spark Connect session id through the active SparkContext's job tags.
   *
   * <p>All reflection — SparkConnectAuthInterceptor lives in a separate extension module
   * (spark-connect-auth) that is optional on the classpath. Missing class means we're in Kyuubi /
   * plain Spark / non-Spark usage and this resolver is a no-op.
   *
   * <p>Only meaningful on the driver. Executors should never hit this path because the credential
   * should already be in TaskContext via SparkContext.setLocalProperty when the task was launched.
   */
  private String resolveFromSparkConnectStore() {
    try {
      Class<?> interceptorClass = Class.forName("com.example.SparkConnectAuthInterceptor$");
      Object module = interceptorClass.getField("MODULE$").get(null);
      Object store = interceptorClass.getMethod("CREDENTIAL_STORE").invoke(module);
      if (store == null) {
        return null;
      }
      @SuppressWarnings("unchecked")
      java.util.concurrent.ConcurrentMap<String, String> credentialStore =
          (java.util.concurrent.ConcurrentMap<String, String>) store;
      if (credentialStore.isEmpty()) {
        return null;
      }

      // Spark Connect tags jobs with "spark.job.tags" = "<sessionId>,<...>". Scan the tag value
      // against the store keys so future tag-format changes don't break this. Reading the tag
      // requires SparkSession.active(), which throws / returns null on worker threads that
      // aren't Spark tasks (e.g. the ForkJoinPool threads Hadoop FS calls run on in Spark
      // Connect). Wrap that lookup in its own try so failure here doesn't block the
      // single-session fallback below.
      String credential = null;
      try {
        Class<?> sparkSessionClass = Class.forName("org.apache.spark.sql.SparkSession");
        Object session = sparkSessionClass.getMethod("active").invoke(null);
        if (session != null) {
          Object sc = session.getClass().getMethod("sparkContext").invoke(session);
          String tags =
              (String)
                  sc.getClass()
                      .getMethod("getLocalProperty", String.class)
                      .invoke(sc, "spark.job.tags");
          if (tags != null && !tags.isEmpty()) {
            for (java.util.Map.Entry<String, String> entry : credentialStore.entrySet()) {
              if (tags.contains(entry.getKey())) {
                credential = entry.getValue();
                break;
              }
            }
          }
        }
      } catch (Throwable ignore) {
        // No active SparkSession on this thread, or Spark Connect internals changed. Keep
        // credential=null and let the single-session fallback try next.
      }

      // Single-session fallback: per-workspace Spark Connect pods typically have one active
      // session at a time, so if the tag-match path doesn't find anything but exactly one
      // credential is cached, that's the one to use. Also the primary path on threads with
      // no SparkSession available (e.g. ForkJoinPool workers driving Hadoop FS).
      if (credential == null && credentialStore.size() == 1) {
        credential = credentialStore.values().iterator().next();
      }

      return credential;
    } catch (Throwable e) {
      return null;
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder
      extends OAuth2TokenProviderBuilder<Builder, GvfsSessionAwareOAuth2TokenProvider> {

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
    protected GvfsSessionAwareOAuth2TokenProvider internalBuild() {
      // credential intentionally optional — session-oauth2 relies on per-user credential from
      // the active SparkSession at token-fetch time; no bootstrap token is pre-fetched, and
      // no admin fallback is wired. getAccessToken() fails fast if nothing resolves.
      Preconditions.checkArgument(
          StringUtils.isNotBlank(scope), "OAuth2TokenProvider must set scope");
      Preconditions.checkArgument(
          StringUtils.isNotBlank(path), "OAuth2TokenProvider must set path");

      GvfsSessionAwareOAuth2TokenProvider provider = new GvfsSessionAwareOAuth2TokenProvider();
      provider.client = client;
      provider.sharedCredential = credential;
      provider.scope = scope;
      provider.path = path;
      return provider;
    }
  }
}
