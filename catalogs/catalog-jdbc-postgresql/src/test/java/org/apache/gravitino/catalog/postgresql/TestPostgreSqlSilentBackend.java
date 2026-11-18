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
package org.apache.gravitino.catalog.postgresql;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.gravitino.catalog.jdbc.config.JdbcConfig;
import org.apache.gravitino.catalog.jdbc.utils.DataSourceUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Verifies with the real PostgreSQL driver that a pooled connection cannot wait forever on a
 * backend that accepts the TCP connection and then stops answering. A catalog operation holds a
 * metadata tree lock while it waits, so an unbounded wait stalls the whole metalake.
 */
public class TestPostgreSqlSilentBackend {

  /** Fails the test, instead of hanging the build, if a borrow is not bounded. */
  private static final Duration HANG_GUARD = Duration.ofSeconds(20);

  /** The one-second timeout under test, less a margin for timer granularity. */
  private static final Duration MIN_WAIT = Duration.ofMillis(900);

  private ServerSocket server;
  private volatile boolean completeStartup;

  /** Starts a backend that accepts every connection and never answers a query. */
  @BeforeEach
  public void startSilentBackend() throws IOException {
    server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
    Thread acceptor = new Thread(this::acceptConnections, "silent-postgres-acceptor");
    acceptor.setDaemon(true);
    acceptor.start();
  }

  /** Stops the backend. */
  @AfterEach
  public void stopSilentBackend() throws IOException {
    server.close();
  }

  /** The login timeout bounds a backend that never answers the startup message. */
  @Test
  public void testLoginTimeoutBoundsSilentStartup() {
    completeStartup = false;
    assertBorrowFails("1000" /* connectTimeoutMs */, "0" /* socketTimeoutMs */);
  }

  /** The socket timeout bounds a backend that never answers the startup message. */
  @Test
  public void testSocketTimeoutBoundsSilentStartup() {
    completeStartup = false;
    assertBorrowFails("60000" /* connectTimeoutMs */, "1000" /* socketTimeoutMs */);
  }

  /** The validation timeout bounds an open connection whose backend stopped answering. */
  @Test
  public void testValidationTimeoutBoundsSilentConnection() {
    completeStartup = true;
    assertBorrowFails("1000" /* connectTimeoutMs */, "0" /* socketTimeoutMs */);
  }

  /** The socket timeout bounds an open connection whose backend stopped answering. */
  @Test
  public void testSocketTimeoutBoundsSilentConnection() {
    completeStartup = true;
    assertBorrowFails("60000" /* connectTimeoutMs */, "1000" /* socketTimeoutMs */);
  }

  private void assertBorrowFails(String connectTimeoutMs, String socketTimeoutMs) {
    Map<String, String> properties = new HashMap<>();
    properties.put(
        JdbcConfig.JDBC_URL.getKey(),
        // assumeMinServerVersion keeps the driver from running setup queries while connecting, so
        // the first statement on an accepted session is the pool's validation.
        "jdbc:postgresql://127.0.0.1:"
            + server.getLocalPort()
            + "/db?sslmode=disable&assumeMinServerVersion=9.0");
    properties.put(JdbcConfig.JDBC_DRIVER.getKey(), "org.postgresql.Driver");
    properties.put(JdbcConfig.USERNAME.getKey(), "user");
    properties.put(JdbcConfig.PASSWORD.getKey(), "password");
    properties.put(JdbcConfig.CONNECT_TIMEOUT_MS.getKey(), connectTimeoutMs);
    properties.put(JdbcConfig.SOCKET_TIMEOUT_MS.getKey(), socketTimeoutMs);

    Assertions.assertTimeoutPreemptively(
        HANG_GUARD,
        () -> {
          try (BasicDataSource dataSource =
              (BasicDataSource) DataSourceUtils.createDataSource(properties)) {
            long start = System.nanoTime();
            Assertions.assertThrows(
                SQLException.class,
                () -> {
                  try (Connection connection = dataSource.getConnection()) {
                    connection.getMetaData().getSchemas().close();
                  }
                });
            // The borrow must have waited for the silent backend and been cut off by the
            // timeout; an earlier failure would mean the driver rejected the backend instead.
            Duration waited = Duration.ofNanos(System.nanoTime() - start);
            Assertions.assertTrue(waited.compareTo(MIN_WAIT) >= 0, "waited only " + waited);
          }
        });
  }

  private void acceptConnections() {
    while (!server.isClosed()) {
      try {
        Socket socket = server.accept();
        Thread handler = new Thread(() -> serve(socket), "silent-postgres-connection");
        handler.setDaemon(true);
        handler.start();
      } catch (IOException e) {
        // The server socket was closed by the test.
        return;
      }
    }
  }

  private void serve(Socket socket) {
    try (Socket connection = socket) {
      DataInputStream in = new DataInputStream(connection.getInputStream());
      if (completeStartup) {
        // Read the startup message, then accept the session without asking for a password.
        int length = in.readInt();
        in.readFully(new byte[length - 4]);
        OutputStream out = connection.getOutputStream();
        out.write(new byte[] {'R', 0, 0, 0, 8, 0, 0, 0, 0}); // AuthenticationOk
        byte[] serverVersion = "server_version\00016.0\000".getBytes(StandardCharsets.US_ASCII);
        out.write(new byte[] {'S', 0, 0, 0, (byte) (4 + serverVersion.length)}); // ParameterStatus
        out.write(serverVersion);
        out.write(new byte[] {'Z', 0, 0, 0, 5, 'I'}); // ReadyForQuery, idle
        out.flush();
      }
      // Keep the connection open and never answer, until the client gives up.
      while (in.read() >= 0) {
        // Discard whatever the client sends.
      }
    } catch (IOException e) {
      // The client closed the connection.
    }
  }
}
