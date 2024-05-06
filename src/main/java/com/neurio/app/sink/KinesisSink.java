package com.neurio.app.sink;

import com.neurio.app.config.AppConfig;
import software.amazon.kinesis.connectors.flink.FlinkKinesisProducer;
import software.amazon.kinesis.connectors.flink.KinesisPartitioner;
import software.amazon.kinesis.connectors.flink.config.AWSConfigConstants;
import software.amazon.kinesis.connectors.flink.serialization.KinesisSerializationSchema;

import java.util.Properties;

public class KinesisSink {

    public static FlinkKinesisProducer getKinesisSink(KinesisSerializationSchema schema, KinesisPartitioner partitioner, String endpoint, String region, String streamName, boolean isDevelopment) {
        Properties kinesisProducerConfigs = new Properties();
        kinesisProducerConfigs.setProperty(AWSConfigConstants.AWS_REGION, region);
        kinesisProducerConfigs.setProperty(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "AUTO");
        if (isDevelopment) {
            // uses localstack
            kinesisProducerConfigs.setProperty("VerifyCertificate", "false");
            kinesisProducerConfigs.setProperty("KinesisPort", endpoint.split(":")[2]);
            kinesisProducerConfigs.setProperty("KinesisEndpoint", "localhost");
        } else {
            kinesisProducerConfigs.setProperty(AWSConfigConstants.AWS_ENDPOINT, endpoint);
        }

        FlinkKinesisProducer sinkFunction = new FlinkKinesisProducer<>(schema, kinesisProducerConfigs);
        sinkFunction.setFailOnError(true);
        sinkFunction.setCustomPartitioner(partitioner);
        sinkFunction.setDefaultStream(streamName);
        return sinkFunction;
    }
}
