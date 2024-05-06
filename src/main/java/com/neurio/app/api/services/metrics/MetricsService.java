package com.neurio.app.api.services.metrics;

import com.neurio.app.config.AppConfig;
import com.neurio.app.utils.Helper;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.util.EntityUtils;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class MetricsService implements IMetricsService {

    private CloseableHttpAsyncClient httpClient;
    private String metricsServiceUrl;
    private String metricsIngestApiKey;
    private String deploymentType;
    private AtomicInteger metricCount;
    private List<BaseMetric> buffer;

    private String resourceNotFoundCountMetricName = "delta.rgm.resourceNotFoundCount";

    public MetricsService(AppConfig.NewRelicConfig newRelicConfig, String deploymentType) {
        this.httpClient = HttpAsyncClients.createDefault();;
        this.metricsServiceUrl = newRelicConfig.getEndpoint();
        this.metricsIngestApiKey = newRelicConfig.getIngestApiKey();
        this.deploymentType = deploymentType;

        this.resourceNotFoundCountMetricName = this.resourceNotFoundCountMetricName + "." + this.deploymentType;

        metricCount = new AtomicInteger(0);
        buffer = new ArrayList<BaseMetric>();

        startAsyncClient();
    }

    public void sendResourceNotFoundCountMetric(String hostRcpn) {
        CountMetric cm = initializeCountMetric();
        cm.setName(this.resourceNotFoundCountMetricName);
        cm.setValue(1);

        Map<String,String> hm = new HashMap<String,String>();
        hm.put("hostRcpn", hostRcpn);
        cm.setAttributes(hm);

        buffer.add(cm);

        if (metricCount.incrementAndGet() >= 100) {
            synchronized (this) {
                try {
                    sendNewRelicPostRequest();
                    metricCount = new AtomicInteger(0);
                    buffer.clear();
                } catch (Exception e) {
                    log.error("Failed to send metrics batch", e);
                }
            }
        }
    }

    private void sendNewRelicPostRequest(){
        MetricsRequestDTO dto = new MetricsRequestDTO();
        dto.setMetrics(buffer);
        List<MetricsRequestDTO> listDtos = new ArrayList<MetricsRequestDTO>();
        listDtos.add(dto);

        try {
            HttpPost httpPost = new HttpPost(this.metricsServiceUrl);
            httpPost.setHeader("Content-Type", "application/json");
            httpPost.setHeader("Api-Key", this.metricsIngestApiKey);

            StringEntity se = new StringEntity(Helper.toJsonString(listDtos));
            httpPost.setEntity(se);
            httpClient.execute(httpPost, new FutureCallback<HttpResponse>() {
                @Override
                public void completed(HttpResponse response) {
                    try {
                            String body = EntityUtils.toString(response.getEntity(), "UTF-8");
                            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK)
                                log.debug("got 200 from {}", httpPost.getURI().toASCIIString());
                            else {
                                log.warn(
                                        "{} responded with status code: {} and body {} ",
                                        metricsServiceUrl,
                                        response.getStatusLine().getStatusCode(),
                                        body);
                            }
                    } catch (Exception e) {
                        log.warn("Exception occured in future completed handler: {}", e.getMessage());
                    }
                }

                @Override
                public void failed(Exception e) {
                    log.warn("Future failed: {}", e.getMessage());
                }

                @Override
                public void cancelled() {
                    log.warn("Future cancelled");
                }
            });
        } catch(Exception e) {
            log.warn("Exception occured on metric request: {}", e.getMessage());
        }
    }

    public void startAsyncClient() {
        httpClient.start();
    }

    private CountMetric initializeCountMetric() {
        CountMetric cm = new CountMetric();
        cm.setType("count");
        cm.setIntervalMs(1000 * 60 * 5);
        cm.setTimestamp(System.currentTimeMillis());
        return cm;
    }
}
