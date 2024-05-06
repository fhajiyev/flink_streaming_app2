package com.neurio.app.api.services.essdp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.generac.ces.essdataprovider.model.PagedResponseDto;
import com.google.gson.reflect.TypeToken;
import com.neurio.app.common.exceptions.Exceptions;
import com.neurio.app.config.AppConfig;
import com.neurio.app.utils.Helper;
import com.neurio.app.dto.SystemEnergyDto;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpStatus;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class EssDpService implements IEssDpService {

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final String essEnergyByStartAndEndTimesEndpoint;
    private final long timeout;

    public EssDpService(HttpClient httpClient, AppConfig.EssDpServiceApiConfig essDpServiceApiConfig) {
        this.httpClient = httpClient;
        this.essEnergyByStartAndEndTimesEndpoint = essDpServiceApiConfig.getEssDpHostUrl() + essDpServiceApiConfig.getEssDpEndpoint();
        this.timeout = essDpServiceApiConfig.getTimeout();
    }

    @Override
    public CompletableFuture<PagedResponseDto<SystemEnergyDto>> getEssEnergyByStartAndEndTimes(String systemId, Long prevTimestamp, Long currTimestamp) {
        HttpRequest request = buildGetEssEnergyByStartAndEndTimesHttpRequest(systemId, prevTimestamp, currTimestamp);

        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenApply(resp -> {
                    if (resp.statusCode() == HttpStatus.SC_OK) {
                        try {
                            return Helper.deserializeJson(resp.body(), new TypeToken<PagedResponseDto<SystemEnergyDto>>(){}.getType());
                        } catch (Exception e) {
                            throw new RuntimeException(
                                    String.format("Failed to parse ess energy response: %s | system id %s", e.getMessage(), systemId));
                        }
                    } else if (resp.statusCode() == HttpStatus.SC_NOT_FOUND) {
                        throw new Exceptions.ResourceNotFoundException("No information found for system id: " + systemId);
                    } else {
                        String errorMsg = String.format(
                                "Failed to get ess energy for system id %s. | " +
                                        "RequestUrl: %s, ResponseBody: %s with statusCode: %s",
                                systemId, request.uri().toString(), resp.body(), resp.statusCode());
                        throw new RuntimeException(errorMsg);
                    }
                });
    }

    public HttpRequest buildGetEssEnergyByStartAndEndTimesHttpRequest(String systemId, Long prevTimestamp, Long currTimestamp) {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
        LocalDateTime ldtPrev = LocalDateTime.ofInstant(Instant.ofEpochSecond(prevTimestamp), ZoneId.of("UTC"));
        LocalDateTime ldtCurr = LocalDateTime.ofInstant(Instant.ofEpochSecond(currTimestamp), ZoneId.of("UTC"));

        return HttpRequest.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .uri(URI.create(String.format(essEnergyByStartAndEndTimesEndpoint + "/%s/system-energy?granularity=5-Minute&startTime=%s&endTime=%s", systemId, ldtPrev.format(dtf), ldtCurr.format(dtf))))
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .GET()
                .timeout(Duration.ofSeconds(timeout))
                .build();
    }
}