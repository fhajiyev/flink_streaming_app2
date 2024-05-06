package com.neurio.app.integration;

import com.neurio.app.App;
import com.neurio.app.config.AppConfig;
import com.neurio.app.testingutils.testcontainer.aws.AwsTestContainer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.org.apache.commons.lang.RandomStringUtils;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.Shard;


import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

@Slf4j
@Tag("IntegrationTest")
@Testcontainers
public abstract class IntegrationTestBase {

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(1)  // this is equivalent to parallelism per kpu
                            .setNumberTaskManagers(2) // this is equivalent to parallelism
                            .build());

    public static final AwsTestContainer AWS_CONTAINER = AwsTestContainer.getInstance();
    private static boolean isConfigured;

    protected static AppConfig testAppConfig;

    @BeforeAll
    static void setupContainers() {
        if(!isConfigured) {
            AWS_CONTAINER.start();
            configureAppConfig();
            isConfigured = true;
        }
    }

    private static void configureAppConfig() {

        testAppConfig = new AppConfig();
        testAppConfig.setLocalDevelopment(true);
        testAppConfig.setGeneralConfig(AppConfig.GeneralConfig.builder()
                .deploymentType("local")
                .build());
        testAppConfig.setKinesisSourceConfig(AppConfig.KinesisSourceConfig.builder()
                .consumerRecordPublisherType("POLLING")
                .initialStreamPosition("TRIM_HORIZON")
                .kinesisEndpoint(AWS_CONTAINER.getEndpointOverride(LocalStackContainer.Service.KINESIS).toString())
                .awsRegion("us-east-1")
                .streamName(RandomStringUtils.randomAlphabetic(3) + "-localstack-input-kinesis-stream")
                .build());
        testAppConfig.setSystemServiceApiConfig(AppConfig.SystemServiceApiConfig.builder()
                .useStub(true)
                .cacheConfig(AppConfig.CacheConfig.builder()
                        .cacheDuration(Duration.ofHours(4).toSeconds())
                        .jitterEnabled(true)
                        .jitterLowerBoundSecond(0L)
                        .jitterUpperBoundSecond(300L)
                        .build())
                .build());
        testAppConfig.setEssDpServiceApiConfig(AppConfig.EssDpServiceApiConfig.builder()
                .useStub(true)
                .build());
        testAppConfig.setDataframeOutputStreamConfig(AppConfig.DataframeOutputStreamConfig.builder()
                .isKinesisSinkEnabled(true)
                .awsRegion("us-east-1")
                .streamName(RandomStringUtils.randomAlphabetic(3) + "-localstack-dataframe-kinesis-sink-stream")
                .kinesisEndpoint(AWS_CONTAINER.getEndpointOverride(LocalStackContainer.Service.KINESIS).toString())
                .build());
        testAppConfig.setPartnerRgmOutputStreamConfig(AppConfig.PartnerRgmOutputStreamConfig.builder()
                .isKinesisSinkEnabled(true)
                .awsRegion("us-east-1")
                .streamName(RandomStringUtils.randomAlphabetic(3) + "-localstack-partner-rgm-kinesis-sink-stream")
                .kinesisEndpoint(AWS_CONTAINER.getEndpointOverride(LocalStackContainer.Service.KINESIS).toString())
                .build());
        testAppConfig.setDlqConfig(AppConfig.DlqConfig.builder()
                .bucket(RandomStringUtils.randomNumeric(3) + "-localstack-asyncfunc-dlq-bucket")
                .build());

    }

    @Getter
    public static class TestDataSet {
        private final boolean isRegistered;
        private ArrayList<String> folderPaths;
        private String timeZone;

        public TestDataSet(String timeZone, boolean isRegistered, String... folders) {
            this.folderPaths = new ArrayList();
            this.isRegistered = isRegistered;
            this.timeZone = timeZone;
            for (String folder : folders) {
                folderPaths.add(folder);
            }
        }
    }

    public static void writeToStream(String streamName, ByteBuffer record, String partitionKey) {

        PutRecordRequest putRecordsRequest = PutRecordRequest.builder()
                .streamName(streamName)
                .partitionKey(partitionKey)
                .data(SdkBytes.fromByteBuffer(record))
                .build();

        AWS_CONTAINER.getKinesisClient().putRecord(putRecordsRequest);

    }

    public static List<ByteBuffer> readFromStream(String streamName) {
        List<ByteBuffer> results = new ArrayList<>();

        // Retrieve the Shards from a Stream
        DescribeStreamRequest describeStreamRequest = DescribeStreamRequest.builder()
                .streamName(streamName)
                .build();

        String shardIterator;
        List<Record> records;

        for (Shard shard : AWS_CONTAINER.getKinesisClient().describeStream(describeStreamRequest).streamDescription().shards()) {
            GetShardIteratorRequest itReq = GetShardIteratorRequest.builder()
                    .streamName(streamName)
                    .shardIteratorType("TRIM_HORIZON")
                    .shardId(shard.shardId())
                    .build();

            GetShardIteratorResponse shardIteratorResult = AWS_CONTAINER.getKinesisClient().getShardIterator(itReq);
            shardIterator = shardIteratorResult.shardIterator();

            while(true) {
                GetRecordsRequest recordsRequest = GetRecordsRequest.builder()
                        .shardIterator(shardIterator)
                        .limit(10)
                        .build();
                GetRecordsResponse result = AWS_CONTAINER.getKinesisClient().getRecords(recordsRequest);
                records = result.records();
                if(records.size() <= 0)
                {
                    break;
                }
                records.forEach(record -> {
                    results.add(record.data().asByteBuffer());
                });
                try {
                    Thread.sleep(10);
                } catch (InterruptedException exception) {
                    throw new RuntimeException(exception);
                }
                shardIterator = result.nextShardIterator();
            }
        }
        return results;
    }

    public void createTestBucketInLocalstack(String bucketName) throws ExecutionException, InterruptedException {
        Assertions.assertTrue(AWS_CONTAINER.getS3AsyncClient().createBucket(builder -> builder.bucket(bucketName)).get().sdkHttpResponse().isSuccessful());
    }

}
