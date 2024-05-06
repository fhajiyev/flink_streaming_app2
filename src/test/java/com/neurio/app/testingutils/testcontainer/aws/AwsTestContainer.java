package com.neurio.app.testingutils.testcontainer.aws;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.kinesis.shaded.com.amazonaws.SDKGlobalConfiguration;

@Slf4j
@Getter
/**
 * A wrapper class around localstackcontainer
 * **/
public class AwsTestContainer extends LocalStackContainer{


    private S3AsyncClient s3AsyncClient;
    private KinesisClient kinesisClient;
    private StaticCredentialsProvider credentialsProvider;

    private AwsTestContainer() {
        super(DockerImageName.parse("localstack/localstack:0.14.5"));
    }

    @Override
    public void start() {
        if (super.isRunning()) {
            return;
        }
        super.withServices()
                .withServices(Service.KINESIS, Service.S3)
                .withReuse(true);

        super.start();
        super.waitingFor(Wait.forHealthcheck());
        credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create(
                this.getAccessKey(), this.getSecretKey()
        ));

        s3AsyncClient = S3AsyncClient
                .builder()
                .endpointOverride(this.getEndpointOverride(Service.S3))
                .credentialsProvider(credentialsProvider)
                .region(Region.of(this.getRegion()))
                .build();

        // need to disable for it to work with localstack
        System.setProperty(SDKGlobalConfiguration.AWS_CBOR_DISABLE_SYSTEM_PROPERTY, "true");
        System.setProperty(SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "true");
        kinesisClient = KinesisClient.builder()
                .endpointOverride(this.getEndpointOverride(Service.KINESIS))
                .credentialsProvider(credentialsProvider)
                .region(Region.of(this.getRegion()))
                .build();

    }
//
//    public static S3AsyncClient S3_ASYNC_CLIENT;
//    public static KinesisClient KINESIS_CLIENT;
//    public static StaticCredentialsProvider LOCAL_STACK_CREDENTIALS;
//    private static boolean isConfigured;
//
//    public static LocalStackContainer getInstance() {
//
//        if (!isConfigured) {
//            LazyHolder.INSTANCE.start();
//            log.info("Starting local stack...");
//            LazyHolder.INSTANCE.waitingFor(Wait.forHealthcheck());
//
//            LOCAL_STACK_CREDENTIALS = StaticCredentialsProvider.create(AwsBasicCredentials.create(
//                    LazyHolder.INSTANCE.getAccessKey(), LazyHolder.INSTANCE.getSecretKey()
//            ));
//
//            S3_ASYNC_CLIENT = S3AsyncClient
//                    .builder()
//                    .endpointOverride(LazyHolder.INSTANCE.getEndpointOverride(LocalStackContainer.Service.S3))
//                    .credentialsProvider(LOCAL_STACK_CREDENTIALS)
//                    .region(Region.of(LazyHolder.INSTANCE.getRegion()))
//                    .build();
//
//            // need to disable for it to work with localstack
//            System.setProperty(SDKGlobalConfiguration.AWS_CBOR_DISABLE_SYSTEM_PROPERTY, "true");
//            System.setProperty(SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "true");
//            KINESIS_CLIENT = KinesisClient.builder()
//                    .endpointOverride(LazyHolder.INSTANCE.getEndpointOverride(LocalStackContainer.Service.KINESIS))
//                    .credentialsProvider(LOCAL_STACK_CREDENTIALS)
//                    .region(Region.of(LazyHolder.INSTANCE.getRegion()))
//                    .build();
//            isConfigured = true;
//        }
//
//        return LazyHolder.INSTANCE;
//    }

    private static class LazyHolder {
        static final AwsTestContainer INSTANCE = new AwsTestContainer();

    }

    public static AwsTestContainer getInstance() {
        return LazyHolder.INSTANCE;
    }

}
