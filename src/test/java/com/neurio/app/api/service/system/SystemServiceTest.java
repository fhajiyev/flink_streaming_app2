package com.neurio.app.api.service.system;

import com.google.gson.JsonObject;
import com.neurio.app.api.services.system.SystemResponseDto;
import com.neurio.app.api.services.system.SystemService;
import com.neurio.app.common.exceptions.Exceptions;
import com.neurio.app.config.AppConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.testcontainers.shaded.org.apache.commons.lang.RandomStringUtils;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Tag("UnitTest")
@Execution(ExecutionMode.SAME_THREAD)
public class SystemServiceTest {


    private static SystemService systemService;
    @Mock
    private static HttpClient httpClient;

    @BeforeAll
    public static void setup() {
        httpClient = Mockito.mock(HttpClient.class);
        AppConfig appConfig = new AppConfig();
        appConfig.setSystemServiceApiConfig(AppConfig.SystemServiceApiConfig.builder()
                .systemHostUrl("http://localhost:9191")
                .systemEndpoint("/system/internal/v1")
                .build());
        systemService = new SystemService(httpClient, appConfig.getSystemServiceApiConfig());
    }

    @Test
    public void testGetSystemByRcpn() {
        String hostrcpn = RandomStringUtils.randomAlphabetic(10);
        HttpRequest request = systemService.buildGetSystemByHostRcpnHttpRequest(hostrcpn);
        HttpResponse httpResponse = Mockito.mock(HttpResponse.class);
        Mockito.when(httpResponse.statusCode()).thenReturn(HttpStatus.SC_OK);
        JsonObject jsonResponse = new JsonObject();
        jsonResponse.addProperty("isRegistered", true);
        jsonResponse.addProperty("timezone", "America/New_York");
        jsonResponse.addProperty("systemId", "systemId-1");
        jsonResponse.addProperty("locationId", "locationId-1");
        jsonResponse.addProperty("hostRcpn", hostrcpn);
        Mockito.when(httpResponse.body()).thenReturn(jsonResponse.toString());
        Mockito.when(httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())).thenReturn(CompletableFuture.completedFuture(httpResponse));


        SystemResponseDto systemResponseDto = systemService.getSystemByHostRcpn(hostrcpn).join();
        Assertions.assertTrue(systemResponseDto.getIsRegistered());
        Assertions.assertEquals(systemResponseDto.getTimezone(), "America/New_York");
        Assertions.assertEquals(systemResponseDto.getSystemId(), "systemId-1");
        Assertions.assertEquals(systemResponseDto.getLocationId(), "locationId-1");
    }


    @Test
    public void testGetSystemByRcpn_notFound() {
        String hostrcpn = RandomStringUtils.randomAlphabetic(10);
        HttpRequest request = systemService.buildGetSystemByHostRcpnHttpRequest(hostrcpn);
        HttpResponse httpResponse = Mockito.mock(HttpResponse.class);
        Mockito.when(httpResponse.statusCode()).thenReturn(HttpStatus.SC_NOT_FOUND);
        JsonObject jsonResponse = new JsonObject();
        jsonResponse.addProperty("msg", "System not found");

        Mockito.when(httpResponse.body()).thenReturn(jsonResponse.toString());
        Mockito.when(httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())).thenReturn(CompletableFuture.completedFuture(httpResponse));

        SystemResponseDto systemResponseDto = systemService.getSystemByHostRcpn(hostrcpn).exceptionally(throwable -> {
            Assertions.assertNotNull(throwable);
            Assertions.assertTrue(throwable.getCause() instanceof Exceptions.ResourceNotFoundException);
            return null;
        }).join();

        Assertions.assertNull(systemResponseDto);
    }

}
