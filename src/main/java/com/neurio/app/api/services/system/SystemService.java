package com.neurio.app.api.services.system;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.neurio.app.common.exceptions.Exceptions;
import com.neurio.app.config.AppConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpStatus;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class SystemService implements ISystemService {

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final String getSystemByHostRcpnEndpoint;

    public SystemService(HttpClient httpClient, AppConfig.SystemServiceApiConfig systemServiceApiConfig) {
        this.httpClient = httpClient;
        this.getSystemByHostRcpnEndpoint = systemServiceApiConfig.getSystemHostUrl() + systemServiceApiConfig.getSystemEndpoint();
    }

    public CompletableFuture<SystemResponseDto> getSystemByHostRcpn(String hostRcpn) {
        HttpRequest request = buildGetSystemByHostRcpnHttpRequest(hostRcpn);
        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenApply((resp) -> {
                    if (resp.statusCode() == HttpStatus.SC_OK) {
                        try {
                            return objectMapper.readValue(resp.body(), SystemResponseDto.class);
                        } catch (JsonProcessingException e) {
                            throw new RuntimeException("Failed to parse response");
                        }
                    } else if (resp.statusCode() == HttpStatus.SC_NOT_FOUND) {
                        throw new Exceptions.ResourceNotFoundException("No system information found for " + hostRcpn);
                    } else {
                        String errorMsg = String.format("Failed to get system details for inverter %s. requestUrl: %s, responseBody: %s with statusCode: %s", hostRcpn, request.uri().toString(), resp.body(), resp.statusCode());
                        throw new RuntimeException(errorMsg);
                    }
                });
    }


    public HttpRequest buildGetSystemByHostRcpnHttpRequest(String hostRcpn) {
        return HttpRequest.newBuilder()
                .uri(URI.create(String.format("%s?hostRcpn=%s", getSystemByHostRcpnEndpoint, hostRcpn)))
                .header("Content-Type", "application/json")
                .GET()
                .timeout(Duration.ofSeconds(5))
                .build();
    }
}