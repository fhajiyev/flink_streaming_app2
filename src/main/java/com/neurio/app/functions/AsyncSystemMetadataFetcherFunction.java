package com.neurio.app.functions;

import com.google.protobuf.util.JsonFormat;
import com.neurio.app.api.services.metrics.IMetricsService;
import com.neurio.app.api.services.metrics.MetricsService;
import com.neurio.app.api.services.metrics.MetricsServiceStub;
import com.neurio.app.api.services.system.ISystemService;
import com.neurio.app.api.services.system.SystemService;
import com.neurio.app.api.services.system.SystemServiceStub;
import com.neurio.app.common.Cache;
import com.neurio.app.common.RedisCache;
import com.neurio.app.common.RedisCacheStub;
import com.neurio.app.common.exceptions.Exceptions;
import com.neurio.app.config.AppConfig;
import com.neurio.app.dataframe.DataFrame;
import com.neurio.app.dataframe.component.SystemMetaData;
import com.neurio.app.protobuf.RecordSetProto;
import com.neurio.app.utils.Helper;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.client.config.SdkAdvancedAsyncClientOption;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.net.http.HttpClient;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Collections;

@Slf4j
public class AsyncSystemMetadataFetcherFunction extends RichAsyncFunction<DataFrame, DataFrame> {

    private final AppConfig.SystemServiceApiConfig systemServiceApiConfig;
    private final AppConfig.DlqConfig dlqConfig;
    private final AppConfig.NewRelicConfig newRelicConfig;
    private final String deploymentType;
    private final boolean isLocalDevelopment;
    private transient ISystemService systemService;
    private transient IMetricsService metricsService;
    private transient Cache<String, SystemMetaData> cache;
    private transient S3AsyncClient client;
    private static final String DUMMY_SYSTEM_META_DATA = "placeholder";

    public AsyncSystemMetadataFetcherFunction(
            AppConfig.SystemServiceApiConfig systemServiceApiConfig,
            AppConfig.DlqConfig dlqConfig,
            AppConfig.NewRelicConfig newRelicConfig,
            String deploymentType,
            boolean isLocalDevelopment
    ) {
        this.systemServiceApiConfig = systemServiceApiConfig;
        this.dlqConfig = dlqConfig;
        this.newRelicConfig = newRelicConfig;
        this.deploymentType = deploymentType;
        this.isLocalDevelopment = isLocalDevelopment;
    }

    @VisibleForTesting
    private void setSystemService(ISystemService systemService) {
        this.systemService = systemService;
    }

    @VisibleForTesting
    private void setCache(Cache cache) {
        this.cache = cache;
    }

    @VisibleForTesting
    private void setMetricsService(IMetricsService metricsService) {
        this.metricsService = metricsService;
    }

    @VisibleForTesting
    private void setS3Client(S3AsyncClient client) {
        this.client = client;
    }

    @Override
    public void asyncInvoke(DataFrame input, ResultFuture<DataFrame> resultFuture) {

        String hostRcpn = input.getEnergyMetaData().getRaw().getHost_rcpn();

        SystemMetaData systemMetaDataCache = cache.get(hostRcpn);

        if (systemMetaDataCache == null) {
            systemService.getSystemByHostRcpn(hostRcpn)
                    .whenCompleteAsync((systemMetaDataDto, throwable) -> {
                        if (throwable != null) {
                            if (throwable.getCause() instanceof Exceptions.ResourceNotFoundException) {
                                // report to newrelic
                                metricsService.sendResourceNotFoundCountMetric(hostRcpn);
                                // send to dlq
                                try {
                                    uploadAsync(input.getEnergyMetaData().getCurrentRecord(), throwable, dlqConfig.getBucket(), Helper.createFilePath(hostRcpn, input.getTimestamp_utc() + 1));
                                } catch (Exception e) {
                                    log.error("Failed to send record to DLQ | hostRcpn: {} | timestamp: {} | exception: {}", hostRcpn, input.getTimestamp_utc(), e.getMessage());
                                    e.printStackTrace();
                                }
                                // store it temporarily in cache
                                log.info("did not find system record for rcpn {}. Storing dummy meta data to prevent overloading system", hostRcpn);
                                cache.set(hostRcpn, SystemMetaData.builder().hostRcpn(DUMMY_SYSTEM_META_DATA).build(), Duration.ofMinutes(15).toSeconds() + getJitter(systemServiceApiConfig.getCacheConfig().getJitterUpperBoundSecond(), systemServiceApiConfig.getCacheConfig().getJitterLowerBoundSecond()));
                                resultFuture.complete(Collections.emptyList());
                                return;
                            } else {
                                resultFuture.completeExceptionally(throwable);
                                return;
                            }
                        }

                        SystemMetaData systemMetaData = systemMetaDataDto.toSystemMetaData();
                        cache.set(hostRcpn, systemMetaData, systemServiceApiConfig.getCacheConfig().getCacheDuration() + getJitter(systemServiceApiConfig.getCacheConfig().getJitterUpperBoundSecond(), systemServiceApiConfig.getCacheConfig().getJitterLowerBoundSecond()));
                        input.setSystemMetaData(systemMetaData);
                        resultFuture.complete(Collections.singleton(input));

                    });
        } else {
            if (systemMetaDataCache.getHostRcpn().equals(DUMMY_SYSTEM_META_DATA)) {
                resultFuture.complete(Collections.emptyList());
                return;
            }

            input.setSystemMetaData(systemMetaDataCache);
            resultFuture.complete(Collections.singleton(input));
        }

    }

    private long getJitter(long upper, long lower) {
        if (systemServiceApiConfig.getCacheConfig().isJitterEnabled()) {
            return (long) (Math.random() * upper + lower);
        }
        return 0;
    }

    @Override
    public void open(Configuration configuration) throws Exception {
        super.open(configuration);
        HttpClient javaHttpClient = HttpClient.newHttpClient();

        if (systemServiceApiConfig.isUseStub()) {
            systemService = new SystemServiceStub();
            cache = new RedisCacheStub<>();
        } else {
            systemService = new SystemService(javaHttpClient, systemServiceApiConfig);
            cache = new RedisCache<>(systemServiceApiConfig.getCacheConfig());
        }

        if(isLocalDevelopment)
            metricsService = new MetricsServiceStub();
        else
            metricsService = new MetricsService(newRelicConfig, deploymentType);

        client = S3AsyncClient.builder()
                .httpClient(NettyNioAsyncHttpClient.builder()
                        .maxConcurrency(100)
                        .maxPendingConnectionAcquires(10_000)
                        .build())
                .asyncConfiguration(
                        conf -> conf.advancedOption(SdkAdvancedAsyncClientOption
                                        .FUTURE_COMPLETION_EXECUTOR,
                                Runnable::run
                        )
                )
                .region(Region.US_EAST_1)
                .build();
    }

    private void uploadAsync(RecordSetProto.RecordSet.Record r, Throwable t, String directory, String filePath) throws Exception {
        DlqRecord dlqRecord = new DlqRecord();
        dlqRecord.setRecord(JsonFormat.printer().print(r));
        dlqRecord.setError(t.getCause().getMessage());
        PutObjectRequest objectRequest = PutObjectRequest.builder()
                .bucket(directory)
                .key(filePath)
                .build();

        // Put the object into the bucket
        client.putObject(objectRequest,
                AsyncRequestBody.fromString(Helper.toJsonString(dlqRecord))
        );
    }

    @Data
    private class DlqRecord {
        private String record;
        private String error;
    }
}
