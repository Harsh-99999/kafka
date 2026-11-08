/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.connect.runtime.rest;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Monitorable;
import org.apache.kafka.common.metrics.PluginMetrics;
import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.connect.rest.ConnectRestExtension;
import org.apache.kafka.connect.rest.ConnectRestExtensionContext;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.MockConnectMetrics;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.isolation.PluginsTest;
import org.apache.kafka.connect.runtime.rest.entities.LoggerLevel;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.eclipse.jetty.client.ContentResponse;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.Request;
import org.eclipse.jetty.client.StringRequestContent;
import org.eclipse.jetty.http.HttpHeader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import jakarta.ws.rs.container.ContainerResponseFilter;
import jakarta.ws.rs.core.MediaType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.reset;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class ConnectRestServerTest {

    @Mock private RestClient restClient;
    @Mock private Herder herder;
    @Mock private Plugins plugins;
    private ConnectRestServer server;
    private HttpClient httpClient;

    protected static final String KAFKA_CLUSTER_ID = "Xbafgnagvar";

    @BeforeEach
    public void setUp() throws Exception {
        httpClient = new HttpClient();
        httpClient.setFollowRedirects(false);
        httpClient.start();
        doReturn(new MockConnectMetrics()).when(herder).connectMetrics();
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (httpClient != null) {
            httpClient.stop();
        }
        if (server != null) {
            server.stop();
        }
    }

    private Map<String, String> baseServerProps() {
        Map<String, String> configMap = new HashMap<>();
        configMap.put(RestServerConfig.LISTENERS_CONFIG, "HTTP://localhost:0");
        return configMap;
    }

    @Test
    public void testCORSEnabled() throws Exception {
        checkCORSRequest("*", "http://bar.com", "http://bar.com", "PUT");
    }

    @Test
    public void testCORSDisabled() throws Exception {
        checkCORSRequest("", "http://bar.com", null, null);
    }

    @Test
    public void testAdvertisedUri() {
        // Clear stubs not needed by this test
        reset(herder);

        // Advertised URI from listeners without protocol
        Map<String, String> configMap = new HashMap<>(baseServerProps());
        configMap.put(RestServerConfig.LISTENERS_CONFIG, "http://localhost:8080,https://localhost:8443");

        server = new ConnectRestServer(null, restClient, configMap);
        assertEquals("http://localhost:8080/", server.advertisedUrl().toString());
        server.stop();

        // Advertised URI from listeners with protocol
        configMap = new HashMap<>(baseServerProps());
        configMap.put(RestServerConfig.LISTENERS_CONFIG, "http://localhost:8080,https://localhost:8443");
        configMap.put(RestServerConfig.REST_ADVERTISED_LISTENER_CONFIG, "https");

        server = new ConnectRestServer(null, restClient, configMap);
        assertEquals("https://localhost:8443/", server.advertisedUrl().toString());
        server.stop();

        // Advertised URI from listeners with only SSL available
        configMap = new HashMap<>(baseServerProps());
        configMap.put(RestServerConfig.LISTENERS_CONFIG, "https://localhost:8443");

        server = new ConnectRestServer(null, restClient, configMap);
        assertEquals("https://localhost:8443/", server.advertisedUrl().toString());
        server.stop();

        // Listener is overridden by advertised values
        configMap = new HashMap<>(baseServerProps());
        configMap.put(RestServerConfig.LISTENERS_CONFIG, "https://localhost:8443");
        configMap.put(RestServerConfig.REST_ADVERTISED_LISTENER_CONFIG, "http");
        configMap.put(RestServerConfig.REST_ADVERTISED_HOST_NAME_CONFIG, "somehost");
        configMap.put(RestServerConfig.REST_ADVERTISED_PORT_CONFIG, "10000");

        server = new ConnectRestServer(null, restClient, configMap);
        assertEquals("http://somehost:10000/", server.advertisedUrl().toString());
        server.stop();

        // correct listener is chosen when https listener is configured before http listener and advertised listener is http
        configMap = new HashMap<>(baseServerProps());
        configMap.put(RestServerConfig.LISTENERS_CONFIG, "https://encrypted-localhost:42069,http://plaintext-localhost:4761");
        configMap.put(RestServerConfig.REST_ADVERTISED_LISTENER_CONFIG, "http");

        server = new ConnectRestServer(null, restClient, configMap);
        assertEquals("http://plaintext-localhost:4761/", server.advertisedUrl().toString());
        server.stop();
    }

    @Test
    public void testOptionsDoesNotIncludeWadlOutput() throws Exception {
        Map<String, String> configMap = new HashMap<>(baseServerProps());

        doReturn(KAFKA_CLUSTER_ID).when(herder).kafkaClusterId();
        doReturn(plugins).when(herder).plugins();
        expectEmptyRestExtensions();

        server = new ConnectRestServer(null, restClient, configMap);
        server.initializeServer();
        server.initializeResources(herder);

        Request request = httpClient.newRequest(server.advertisedUrl().resolve("/connectors")).method("OPTIONS");
        request.headers(headers -> headers.add(HttpHeader.CONTENT_TYPE, MediaType.WILDCARD));
        ContentResponse response = request.send();
        assertEquals(MediaType.TEXT_PLAIN, response.getHeaders().get(HttpHeader.CONTENT_TYPE));
        assertEquals(
            Set.copyOf(response.getHeaders().getCSV(HttpHeader.ALLOW, false)),
            Set.of(response.getContentAsString().split(", "))
        );
    }

    public void checkCORSRequest(String corsDomain, String origin, String expectedHeader, String method)
        throws Exception {
        Map<String, String> configMap = baseServerProps();
        configMap.put(RestServerConfig.ACCESS_CONTROL_ALLOW_ORIGIN_CONFIG, corsDomain);
        configMap.put(RestServerConfig.ACCESS_CONTROL_ALLOW_METHODS_CONFIG, method);

        doReturn(KAFKA_CLUSTER_ID).when(herder).kafkaClusterId();
        doReturn(plugins).when(herder).plugins();
        expectEmptyRestExtensions();
        doReturn(List.of("a", "b")).when(herder).connectors();

        server = new ConnectRestServer(null, restClient, configMap);
        server.initializeServer();
        server.initializeResources(herder);
        URI serverUrl = server.advertisedUrl();

        Request request = httpClient.newRequest(server.advertisedUrl().resolve("/connectors"));
        request.headers(headers -> headers.add("Referer", origin + "/page"));
        request.headers(headers -> headers.add("Origin", origin));
        ContentResponse response = request.send();

        assertEquals(200, response.getStatus());

        if (expectedHeader != null) {
            assertEquals(expectedHeader,
                response.getHeaders().get("Access-Control-Allow-Origin"));
        }

        request = httpClient.newRequest(serverUrl.resolve("/connector-plugins/FileStreamSource/validate")).method("OPTIONS");
        request.headers(headers -> headers.add("Referer", origin + "/page"));
        request.headers(headers -> headers.add("Origin", origin));
        request.headers(headers -> headers.add("Access-Control-Request-Method", method));
        response = request.send();
        assertEquals(404, response.getStatus());
        if (expectedHeader != null) {
            assertEquals(expectedHeader,
                response.getHeaders().get("Access-Control-Allow-Origin"));
        }
        if (method != null) {
            assertEquals(method,
                response.getHeaders().get("Access-Control-Allow-Methods"));
        }
    }

    @Test
    public void testStandaloneConfig() throws Exception  {
        Map<String, String> configMap = baseServerProps();
        configMap.put("offset.storage.file.filename", "/tmp");

        doReturn(KAFKA_CLUSTER_ID).when(herder).kafkaClusterId();
        doReturn(plugins).when(herder).plugins();
        expectEmptyRestExtensions();
        doReturn(List.of("a", "b")).when(herder).connectors();

        server = new ConnectRestServer(null, restClient, configMap);
        server.initializeServer();
        server.initializeResources(herder);
        Request request = httpClient.newRequest(server.advertisedUrl().resolve("/connectors"));
        ContentResponse response = request.send();

        assertEquals(200, response.getStatus());
    }

    @Test
    public void testLoggerEndpointWithDefaults() throws Exception {
        Map<String, String> configMap = new HashMap<>(baseServerProps());

        final String logger = "a.b.c.s.W";
        final String loggingLevel = "INFO";
        final long lastModified = 789052637671L;

        doReturn(KAFKA_CLUSTER_ID).when(herder).kafkaClusterId();
        doReturn(plugins).when(herder).plugins();
        expectEmptyRestExtensions();
        doReturn(List.of()).when(herder).setWorkerLoggerLevel(logger, loggingLevel);
        doReturn(Map.of(logger, new LoggerLevel(loggingLevel, lastModified))).when(herder).allLoggerLevels();

        server = new ConnectRestServer(null, restClient, configMap);
        server.initializeServer();
        server.initializeResources(herder);

        ObjectMapper mapper = new ObjectMapper();

        URI serverUrl = server.advertisedUrl();

        executePut(serverUrl, "/admin/loggers/" + logger, "{\"level\": \"" + loggingLevel + "\"}");

        String responseStr = executeGet(serverUrl, "/admin/loggers");

        Map<String, Object> expectedLogger = new HashMap<>();
        expectedLogger.put("level", loggingLevel);
        expectedLogger.put("last_modified", lastModified);
        Map<String, Map<String, Object>> expectedLoggers = Map.of(logger, expectedLogger);
        Map<String, Map<String, Object>> actualLoggers = mapper.readValue(responseStr, new TypeReference<>() { });
        assertEquals(expectedLoggers, actualLoggers);
    }

    @Test
    public void testIndependentAdminEndpoint() throws Exception {
        Map<String, String> configMap = new HashMap<>(baseServerProps());
        configMap.put(RestServerConfig.ADMIN_LISTENERS_CONFIG, "http://localhost:0");

        doReturn(KAFKA_CLUSTER_ID).when(herder).kafkaClusterId();
        doReturn(plugins).when(herder).plugins();
        expectEmptyRestExtensions();

        // create some loggers in the process
        LoggerFactory.getLogger("a.b.c.s.W");
        LoggerFactory.getLogger("a.b.c.p.X");
        LoggerFactory.getLogger("a.b.c.p.Y");
        LoggerFactory.getLogger("a.b.c.p.Z");

        server = new ConnectRestServer(null, restClient, configMap);
        server.initializeServer();
        server.initializeResources(herder);

        assertNotEquals(server.advertisedUrl(), server.adminUrl());

        executeGet(server.adminUrl(), "/admin/loggers");

        Request request = httpClient.newRequest(server.advertisedUrl().resolve("/admin/loggers"));
        ContentResponse response = request.send();
        assertEquals(404, response.getStatus());
    }

    @Test
    public void testRestExtensionsRegisteredWithAdminListener() throws Exception {
        Map<String, String> configMap = new HashMap<>(baseServerProps());
        configMap.put(RestServerConfig.ADMIN_LISTENERS_CONFIG, "http://localhost:0");
        configMap.put(RestServerConfig.REST_EXTENSION_CLASSES_CONFIG, HeaderConnectRestExtension.class.getName());

        HeaderConnectRestExtension extension = new HeaderConnectRestExtension();
        doReturn(KAFKA_CLUSTER_ID).when(herder).kafkaClusterId();
        doReturn(plugins).when(herder).plugins();
        doReturn(List.of(extension)).when(plugins).newPlugins(any(), any(), eq(ConnectRestExtension.class));
        doReturn(List.of()).when(herder).connectors();

        server = new ConnectRestServer(null, restClient, configMap);
        server.initializeServer();
        server.initializeResources(herder);

        assertNotEquals(server.advertisedUrl(), server.adminUrl());
        assertEquals(2, extension.registrations);

        ContentResponse regularResponse = httpClient.GET(server.advertisedUrl().resolve("/connectors"));
        assertEquals(200, regularResponse.getStatus());
        assertNotNull(regularResponse.getHeaders().get(extension.name));

        ContentResponse adminResponse = httpClient.GET(server.adminUrl().resolve("/admin/loggers"));
        assertEquals(200, adminResponse.getStatus());
        assertNotNull(adminResponse.getHeaders().get(extension.name));
    }

    @Test
    public void testRestExtensionsWithoutAdminListener() throws Exception {
        Map<String, String> configMap = new HashMap<>(baseServerProps());
        configMap.put(RestServerConfig.REST_EXTENSION_CLASSES_CONFIG, HeaderConnectRestExtension.class.getName());

        HeaderConnectRestExtension extension = new HeaderConnectRestExtension();
        doReturn(KAFKA_CLUSTER_ID).when(herder).kafkaClusterId();
        doReturn(plugins).when(herder).plugins();
        doReturn(List.of(extension)).when(plugins).newPlugins(any(), any(), eq(ConnectRestExtension.class));
        doReturn(List.of()).when(herder).connectors();

        server = new ConnectRestServer(null, restClient, configMap);
        server.initializeServer();
        server.initializeResources(herder);

        assertEquals(1, extension.registrations);

        ContentResponse response = httpClient.GET(server.advertisedUrl().resolve("/connectors"));
        assertEquals(200, response.getStatus());
        assertNotNull(response.getHeaders().get(extension.name));
    }

    @Test
    public void testRestExtensionsWithDisabledAdminListener() throws Exception {
        Map<String, String> configMap = new HashMap<>(baseServerProps());
        configMap.put(RestServerConfig.ADMIN_LISTENERS_CONFIG, "");
        configMap.put(RestServerConfig.REST_EXTENSION_CLASSES_CONFIG, HeaderConnectRestExtension.class.getName());

        HeaderConnectRestExtension extension = new HeaderConnectRestExtension();
        doReturn(KAFKA_CLUSTER_ID).when(herder).kafkaClusterId();
        doReturn(plugins).when(herder).plugins();
        doReturn(List.of(extension)).when(plugins).newPlugins(any(), any(), eq(ConnectRestExtension.class));
        doReturn(List.of()).when(herder).connectors();

        server = new ConnectRestServer(null, restClient, configMap);
        server.initializeServer();
        server.initializeResources(herder);

        assertEquals(1, extension.registrations);

        // Extensions should still work on the regular listener
        ContentResponse response = httpClient.GET(server.advertisedUrl().resolve("/connectors"));
        assertEquals(200, response.getStatus());
        assertNotNull(response.getHeaders().get(extension.name));
    }

    @Test
    public void testDisableAdminEndpoint() throws Exception {
        Map<String, String> configMap = new HashMap<>(baseServerProps());
        configMap.put(RestServerConfig.ADMIN_LISTENERS_CONFIG, "");

        doReturn(KAFKA_CLUSTER_ID).when(herder).kafkaClusterId();
        doReturn(plugins).when(herder).plugins();
        expectEmptyRestExtensions();

        server = new ConnectRestServer(null, restClient, configMap);
        server.initializeServer();
        server.initializeResources(herder);

        assertNull(server.adminUrl());

        Request request = httpClient.newRequest(server.advertisedUrl().resolve("/admin/loggers"));
        ContentResponse response = request.send();
        assertEquals(404, response.getStatus());
    }

    @Test
    public void testRequestLogs() throws Exception {
        Map<String, String> configMap = new HashMap<>(baseServerProps());

        doReturn(KAFKA_CLUSTER_ID).when(herder).kafkaClusterId();
        doReturn(plugins).when(herder).plugins();
        expectEmptyRestExtensions();

        server = new ConnectRestServer(null, restClient, configMap);
        server.initializeServer();
        server.initializeResources(herder);

        LogCaptureAppender restServerAppender = LogCaptureAppender.createAndRegister();
        Request request = httpClient.newRequest(server.advertisedUrl().resolve("/"));
        ContentResponse response = request.send();

        // Stop the server to flush all logs
        server.stop();

        Collection<String> logMessages = restServerAppender.getMessages();
        restServerAppender.close();
        String expectedlogContent = "\"GET / HTTP/1.1\" " + response.getStatus();
        assertTrue(logMessages.stream().anyMatch(logMessage -> logMessage.contains(expectedlogContent)));
    }

    @Test
    public void testValidCustomizedHttpResponseHeaders() throws Exception  {
        String headerConfig =
                "add X-XSS-Protection: 1; mode=block, \"add Cache-Control: no-cache, no-store, must-revalidate\"";
        Map<String, String> expectedHeaders = new HashMap<>();
        expectedHeaders.put("X-XSS-Protection", "1; mode=block");
        expectedHeaders.put("Cache-Control", "no-cache, no-store, must-revalidate");
        checkCustomizedHttpResponseHeaders(headerConfig, expectedHeaders);
    }

    @Test
    public void testDefaultCustomizedHttpResponseHeaders() throws Exception  {
        String headerConfig = "";
        Map<String, String> expectedHeaders = new HashMap<>();
        checkCustomizedHttpResponseHeaders(headerConfig, expectedHeaders);
    }

    static final class HeaderConnectRestExtension extends PluginsTest.TestConnectRestExtension {
        String name = "X-Header";
        int registrations = 0;

        @Override
        public void register(ConnectRestExtensionContext restPluginContext) {
            registrations++;
            restPluginContext.configurable().register(
                (ContainerResponseFilter) (req, resp) ->
                    resp.getHeaders().add(name, "true")
            );
        }
    }

    static final class MonitorableConnectRestExtension extends PluginsTest.TestConnectRestExtension implements Monitorable {

        private boolean called = false;
        private static MetricName metricName;

        @Override
        public void register(ConnectRestExtensionContext restPluginContext) {
            called = true;
        }

        @Override
        public void withPluginMetrics(PluginMetrics metrics) {
            metricName = metrics.metricName("name", "description", new LinkedHashMap<>());
            metrics.addMetric(metricName, (Gauge<Boolean>) (config, now) -> called);
        }
    }

    @Test
    public void testMonitorableConnectRestExtension() {
        Map<String, String> configMap = new HashMap<>(baseServerProps());
        configMap.put(RestServerConfig.REST_EXTENSION_CLASSES_CONFIG, MonitorableConnectRestExtension.class.getName());

        doReturn(plugins).when(herder).plugins();
        doReturn(List.of(new MonitorableConnectRestExtension())).when(plugins).newPlugins(any(), any(), eq(ConnectRestExtension.class));

        server = new ConnectRestServer(null, restClient, configMap);
        server.initializeServer();
        server.initializeResources(herder);

        Map<MetricName, KafkaMetric> metrics = herder.connectMetrics().metrics().metrics();
        assertTrue(metrics.containsKey(MonitorableConnectRestExtension.metricName));
        assertTrue((boolean) metrics.get(MonitorableConnectRestExtension.metricName).metricValue());

        server.stop();
        metrics = herder.connectMetrics().metrics().metrics();
        assertFalse(metrics.containsKey(MonitorableConnectRestExtension.metricName));
    }

    private void checkCustomizedHttpResponseHeaders(String headerConfig, Map<String, String> expectedHeaders)
            throws Exception  {
        Map<String, String> configMap = baseServerProps();
        configMap.put("offset.storage.file.filename", "/tmp");
        configMap.put(RestServerConfig.RESPONSE_HTTP_HEADERS_CONFIG, headerConfig);

        doReturn(KAFKA_CLUSTER_ID).when(herder).kafkaClusterId();
        doReturn(plugins).when(herder).plugins();
        expectEmptyRestExtensions();
        doReturn(List.of("a", "b")).when(herder).connectors();

        server = new ConnectRestServer(null, restClient, configMap);
        server.initializeServer();
        server.initializeResources(herder);
        Request request = httpClient.newRequest(server.advertisedUrl().resolve("/connectors"));
        ContentResponse response = request.send();
        assertEquals(200, response.getStatus());
        if (!headerConfig.isEmpty()) {
            expectedHeaders.forEach((k, v) ->
                    assertEquals(response.getHeaders().get(k), v));
        } else {
            assertNull(response.getHeaders().get("X-Frame-Options"));
        }
    }

    private String executeGet(URI serverUrl, String endpoint) throws Exception {
        ContentResponse response = httpClient.GET(serverUrl.resolve(endpoint));

        assertEquals(200, response.getStatus());
        return response.getContentAsString();
    }

    private String executePut(URI serverUrl, String endpoint, String jsonBody) throws Exception {
        ContentResponse response = httpClient.newRequest(serverUrl.resolve(endpoint))
                .method("PUT")
                .body(new StringRequestContent("application/json", jsonBody, StandardCharsets.UTF_8))
                .send();

        assertEquals(200, response.getStatus());
        return response.getContentAsString();
    }

    private void expectEmptyRestExtensions() {
        doReturn(List.of()).when(plugins).newPlugins(
                eq(List.of()),
                any(AbstractConfig.class),
                eq(ConnectRestExtension.class)
        );
    }
}
