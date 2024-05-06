package com.neurio.app.api.service.essdp;

import com.generac.ces.essdataprovider.model.PagedResponseDto;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.neurio.app.api.services.essdp.EssDpService;
import com.neurio.app.common.exceptions.Exceptions;
import com.neurio.app.config.AppConfig;
import com.neurio.app.dto.SystemEnergyDto;
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
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Tag("UnitTest")
@Execution(ExecutionMode.SAME_THREAD)
public class EssDpServiceTest {


    private static EssDpService essDpService;
    @Mock
    private static HttpClient httpClient;

    @BeforeAll
    public static void setup() {
        httpClient = Mockito.mock(HttpClient.class);
        AppConfig appConfig = new AppConfig();
        appConfig.setEssDpServiceApiConfig(AppConfig.EssDpServiceApiConfig.builder()
                .essDpHostUrl("http://localhost:8181")
                .essDpEndpoint("/ess-dataprovider/v1/systems")
                .timeout(Duration.ofSeconds(5).toSeconds())
                .build());
        essDpService = new EssDpService(httpClient, appConfig.getEssDpServiceApiConfig());
    }

    @Test
    public void testGetEssEnergyByStartAndEndTimes() throws Exception {
        String systemId = RandomStringUtils.randomAlphabetic(10);
        HttpRequest request = essDpService.buildGetEssEnergyByStartAndEndTimesHttpRequest(systemId, 1621949100L, 1621949400L);
        HttpResponse httpResponse = Mockito.mock(HttpResponse.class);
        Mockito.when(httpResponse.statusCode()).thenReturn(HttpStatus.SC_OK);
        JsonObject pagedResponseDto = new JsonObject();
        JsonArray data = new JsonArray();
        JsonObject systemEnergyDto = new JsonObject();
        JsonObject raw = new JsonObject();
        JsonObject inverter = new JsonObject();
        inverter.addProperty("currentImport_Ws", 12345);
        inverter.addProperty("currentExport_Ws", 54321);
        raw.add("inverter", inverter);
        systemEnergyDto.add("raw", raw);
        data.add(systemEnergyDto);
        pagedResponseDto.add("data", data);

        Mockito.when(httpResponse.body()).thenReturn(pagedResponseDto.toString());
        Mockito.when(httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())).thenReturn(CompletableFuture.completedFuture(httpResponse));

        SystemEnergyDto dto = essDpService.getEssEnergyByStartAndEndTimes(systemId, 1621949100L, 1621949400L).get().getData().get(0);
        Assertions.assertEquals(dto.getRaw().getInverter().getCurrentImport_Ws(), 12345);
        Assertions.assertEquals(dto.getRaw().getInverter().getCurrentExport_Ws(), 54321);
    }


    @Test
    public void testGetEssEnergyByStartAndEndTimes_notFound() throws Exception {
        String systemId = RandomStringUtils.randomAlphabetic(10);
        HttpRequest request = essDpService.buildGetEssEnergyByStartAndEndTimesHttpRequest(systemId, 1621949100L, 1621949400L);
        HttpResponse httpResponse = Mockito.mock(HttpResponse.class);
        Mockito.when(httpResponse.statusCode()).thenReturn(HttpStatus.SC_NOT_FOUND);
        JsonObject jsonResponse = new JsonObject();
        jsonResponse.addProperty("msg", "Ess energy record not found");

        Mockito.when(httpResponse.body()).thenReturn(jsonResponse.toString());
        Mockito.when(httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())).thenReturn(CompletableFuture.completedFuture(httpResponse));

        PagedResponseDto<SystemEnergyDto> dto = essDpService.getEssEnergyByStartAndEndTimes(systemId, 1621949100L, 1621949400L).exceptionally(throwable -> {
            Assertions.assertNotNull(throwable);
            Assertions.assertTrue(throwable.getCause() instanceof Exceptions.ResourceNotFoundException);
            return null;
        }).get();

        Assertions.assertNull(dto);
    }
}
