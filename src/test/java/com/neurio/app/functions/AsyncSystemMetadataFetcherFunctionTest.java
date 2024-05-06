package com.neurio.app.functions;

import com.neurio.app.api.services.metrics.IMetricsService;
import com.neurio.app.api.services.metrics.MetricsServiceStub;
import com.neurio.app.api.services.system.ISystemService;
import com.neurio.app.api.services.system.SystemService;
import com.neurio.app.api.services.system.SystemServiceStub;
import com.neurio.app.common.Cache;
import com.neurio.app.common.RedisCacheStub;
import com.neurio.app.config.AppConfig;
import com.neurio.app.dataframe.DataFrame;
import com.neurio.app.dataframe.component.EnergyMetaData;
import com.neurio.app.dataframe.component.Raw;
import com.neurio.app.dataframe.component.SystemMetaData;
import com.neurio.app.integration.IntegrationTestBase;
import com.neurio.app.protobuf.RecordSetProto;
import com.neurio.app.testingutils.RecordSetProtoProvider;
import com.neurio.app.utils.Helper;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.SocketPolicy;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.client.config.SdkAdvancedAsyncClientOption;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

@Slf4j
@Tag("IntegrationTest")
@ExtendWith(MockitoExtension.class)
public class AsyncSystemMetadataFetcherFunctionTest extends IntegrationTestBase {

    @Spy
    private SystemServiceStub systemServiceStub = new SystemServiceStub();

    private Cache cacheStub = new RedisCacheStub();

    private MetricsServiceStub metricsServiceStub = new MetricsServiceStub();

    private S3AsyncClient s3Client = AWS_CONTAINER.getS3AsyncClient();

    static Stream<Arguments> asyncInvokeTestDataSet() throws IOException {
        return Stream.of(
//                Arguments.of(
//                        // records come in at over a period of time (ie when stream has lots of buffered data), we expect only a single invocation to system as the subsequent requests should be cached.
//                        new SystemMetaDataFetchTestDataSet(5, true, Duration.ofSeconds(1), 5, Duration.ofSeconds(30))
//                ),
//                Arguments.of(
//                        // all records come in at the same time (ie when stream has lots of buffered data), we expect mutiple invocation to system before the result gets cached.
//                        new SystemMetaDataFetchTestDataSet(4, true, Duration.ZERO, 4, Duration.ofSeconds(30))
//                ),
//                Arguments.of(
//                        // 3 records come (same rcpn) in at the same time (ie when stream has lots of buffered data), we expect multiple invocation to system as they are all unique.
//                        new SystemMetaDataFetchTestDataSet(3, false, Duration.ZERO, 1, Duration.ofSeconds(30))
//                ),
                Arguments.of(
                        // 3 unique records come in at the same time (ie when stream has lots of buffered data), we expect multiple invocation to system as they are all unique.
                        new SystemMetaDataFetchTestDataSet(6, false, Duration.ofSeconds(1), 1, Duration.ofSeconds(30))
                )
//                Arguments.of(
//                        // test cache expiry. record comes later than the configured cache ttl, expect a second invocation to system
//                        new SystemMetaDataFetchTestDataSet(2, false, Duration.ofSeconds(5), 2, Duration.ofSeconds(2))
//                )
                );
    }

    @AfterEach
    public void cleanupCache() {

        cacheStub.clearAll();
    }

    @ParameterizedTest
    @MethodSource("asyncInvokeTestDataSet")
    @DisplayName("Tests asyncInvoke without failures")
    @Execution(ExecutionMode.CONCURRENT)
    public void asyncInvokeTestWithSampleRcpnMultipleTimes(SystemMetaDataFetchTestDataSet testDataSet) throws Exception {

        AsyncSystemMetadataFetcherFunction classUnderTest = testDataSet.classUnderTest;

        // use reflection to set private members to spy on stub. Unfortunately flink operators do not support dependency injection
        Method pMethod = AsyncSystemMetadataFetcherFunction.class.getDeclaredMethod("setSystemService", ISystemService.class);
        pMethod.setAccessible(true);
        pMethod.invoke(classUnderTest, systemServiceStub);

        pMethod = AsyncSystemMetadataFetcherFunction.class.getDeclaredMethod("setCache", Cache.class);
        pMethod.setAccessible(true);
        pMethod.invoke(classUnderTest, cacheStub);

        pMethod = AsyncSystemMetadataFetcherFunction.class.getDeclaredMethod("setMetricsService", IMetricsService.class);
        pMethod.setAccessible(true);
        pMethod.invoke(classUnderTest, metricsServiceStub);

        TestableResultFuture<DataFrame> output = new TestableResultFuture();

        for (DataFrame df : testDataSet.dataFrames) {
            classUnderTest.asyncInvoke(df, output);
            Thread.sleep(testDataSet.duration.toMillis());
        }

        // sleep to let the worker threads to collect results to the output
        Thread.sleep(1000);
        if (output.list.size() == 0) {
            Assertions.fail("Expected some elements in output, but found none");
        } else {
            Assertions.assertEquals(testDataSet.dataFrames.size(), output.list.size());
            Mockito.verify(systemServiceStub, Mockito.times(testDataSet.expectedSystemServiceInvocation)).getSystemByHostRcpn(Mockito.anyString());
            output.list.forEach(dataFrame -> {
                SystemMetaData actual = dataFrame.getSystemMetaData();
                SystemMetaData expected = systemServiceStub.getSystemByHostRcpn(dataFrame.getSystemMetaData().getHostRcpn()).join().toSystemMetaData();

                Assertions.assertEquals(expected, actual);
            });
        }
    }

    @ParameterizedTest
    @MethodSource("asyncInvokeTestDataSet")
    @DisplayName("Tests asyncInvoke dlq on ResourceNotFoundException")
    @Execution(ExecutionMode.CONCURRENT)
    public void asyncInvokeTestOnResourceNotFoundException(SystemMetaDataFetchTestDataSet testDataSet) throws Exception {
        createTestBucketInLocalstack(testAppConfig.getDlqConfig().getBucket());

        MockWebServer mockBackEnd = new MockWebServer();
        mockBackEnd.start();
        mockBackEnd.enqueue(new MockResponse().setStatus("HTTP/1.1 404")); // simulate "not found" response

        AsyncSystemMetadataFetcherFunction classUnderTest = testDataSet.classUnderTest;

        // use reflection to set private members to spy on stub. Unfortunately flink operators do not support dependency injection
        Method pMethod = AsyncSystemMetadataFetcherFunction.class.getDeclaredMethod("setSystemService", ISystemService.class);
        pMethod.setAccessible(true);
        pMethod.invoke(classUnderTest, new SystemService(HttpClient.newBuilder().build(), AppConfig.SystemServiceApiConfig.builder().systemHostUrl("http://" + mockBackEnd.getHostName() + ":" + mockBackEnd.getPort()).systemEndpoint("/system/internal/v1").build()));

        pMethod = AsyncSystemMetadataFetcherFunction.class.getDeclaredMethod("setCache", Cache.class);
        pMethod.setAccessible(true);
        pMethod.invoke(classUnderTest, cacheStub);

        pMethod = AsyncSystemMetadataFetcherFunction.class.getDeclaredMethod("setMetricsService", IMetricsService.class);
        pMethod.setAccessible(true);
        pMethod.invoke(classUnderTest, metricsServiceStub);

        pMethod = AsyncSystemMetadataFetcherFunction.class.getDeclaredMethod("setS3Client", S3AsyncClient.class);
        pMethod.setAccessible(true);
        pMethod.invoke(classUnderTest, s3Client);

        TestableResultFuture<DataFrame> output = new TestableResultFuture();

        DataFrame inputDf = testDataSet.dataFrames.get(0);
        classUnderTest.asyncInvoke(inputDf, output);

        // sleep to let the worker threads to collect results to the output
        Thread.sleep(1000);

        Assertions.assertEquals(output.list.size(), 0);

        String expectedDlqFilePath = Helper.createFilePath(inputDf.getEnergyMetaData().getRaw().getHost_rcpn(), inputDf.getTimestamp_utc()+1);

        Assertions.assertTrue(objectExist(s3Client, testAppConfig.getDlqConfig().getBucket(), expectedDlqFilePath));
    }

    public static class SystemMetaDataFetchTestDataSet {
        private List<DataFrame> dataFrames;
        private Duration duration;
        private int expectedSystemServiceInvocation;
        private AsyncSystemMetadataFetcherFunction classUnderTest;

        public SystemMetaDataFetchTestDataSet(int numberOfDataFrames, boolean uniqueRcpn, Duration durationBetweenEachInvoke, int expectedSystemServiceInvocation, Duration cacheTtl) throws IOException {

            AppConfig.SystemServiceApiConfig systemServiceApiConfig = AppConfig.SystemServiceApiConfig
                    .builder()
                    .useStub(true)
                    .cacheConfig(AppConfig.CacheConfig.builder()
                            .cacheDuration(cacheTtl.toSeconds())
                            .jitterEnabled(false)
                            .build())
                    .build();
            AppConfig.DlqConfig dlqConfig = AppConfig.DlqConfig
                    .builder()
                    .bucket(testAppConfig.getDlqConfig().getBucket())
                    .build();

            classUnderTest = new AsyncSystemMetadataFetcherFunction(systemServiceApiConfig, dlqConfig, null, null, true);
            dataFrames = new ArrayList<>();
            String filepath = AsyncSystemMetadataFetcherFunction.class.getResource("/samples/recordsetproto/0001001200C1/2021-03-25/1616544000.json").getPath();
            RecordSetProto.RecordSet recordset = RecordSetProtoProvider.getRecordSetFromFile(filepath);
            for (int i = 0; i < numberOfDataFrames; i++) {

                DataFrame df = new DataFrame();
                df.setEnergyMetaData(EnergyMetaData.builder()
                        .raw(Raw.from(recordset.getRecord(0)))
                        .currentRecord(recordset.getRecord(0))
                        .build());

                if (!uniqueRcpn) {
                    df.getEnergyMetaData().getRaw().setHost_rcpn("rcpn-1");
                } else {
                    df.getEnergyMetaData().getRaw().setHost_rcpn("rcpn-" + i);
                }

                dataFrames.add(df);
            }

            duration = durationBetweenEachInvoke;
            this.expectedSystemServiceInvocation = expectedSystemServiceInvocation;

        }
    }

    public static class TestableResultFuture<T> implements ResultFuture<T> {

        // collects all elements seen by the result future
        public List<T> list = new ArrayList<>();

        @Override
        public void complete(Collection<T> result) {
            result.iterator().forEachRemaining(r -> {
                log.info("adding result to list");
                list.add(r);
            });
        }

        @Override
        public void completeExceptionally(Throwable error) {

        }
    };

    private boolean objectExist(S3AsyncClient client, String directory, String filePath) {
        try {
            HeadObjectResponse response = client.headObject(HeadObjectRequest.builder().bucket(directory).key(filePath).build()).get();
            return response.sdkHttpResponse().isSuccessful();
        } catch (Exception e) {
            return false;
        }
    }
}
