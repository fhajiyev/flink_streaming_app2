package com.neurio.app.source.kinesis;

import com.neurio.app.config.AppConfig;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import software.amazon.kinesis.connectors.flink.FlinkKinesisConsumer;
import software.amazon.kinesis.connectors.flink.config.AWSConfigConstants;
import software.amazon.kinesis.connectors.flink.config.ConsumerConfigConstants;

import java.util.Properties;

@Getter
@Slf4j
public class KinesisSourceConsumer {

    public static <T> FlinkKinesisConsumer<T> getKinesisSource(AppConfig appConfig, AbstractDeserializationSchema<T> deserializationSchema){

        AppConfig.KinesisSourceConfig conf = appConfig.getKinesisSourceConfig();
        Properties kinesisProperties = new Properties();
        kinesisProperties.setProperty(AWSConfigConstants.AWS_REGION, conf.getAwsRegion());
        kinesisProperties.setProperty(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "AUTO");
        kinesisProperties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, conf.getInitialStreamPosition());

        if (ConsumerConfigConstants.InitialPosition.AT_TIMESTAMP.name().equals(conf.getInitialStreamPosition())) {
            kinesisProperties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_TIMESTAMP, conf.getInitialStreamTimestamp());
        }

        kinesisProperties.setProperty(ConsumerConfigConstants.SHARD_GETRECORDS_BACKOFF_BASE, "5000");
        kinesisProperties.setProperty(ConsumerConfigConstants.SHARD_GETITERATOR_BACKOFF_BASE, "5000");
        kinesisProperties.setProperty(ConsumerConfigConstants.SHARD_GETRECORDS_MAX, "1000");
        kinesisProperties.setProperty(ConsumerConfigConstants.SHARD_GETRECORDS_INTERVAL_MILLIS, "5000");

        kinesisProperties.setProperty(ConsumerConfigConstants.AWS_ENDPOINT, conf.getKinesisEndpoint());
        kinesisProperties.setProperty(ConsumerConfigConstants.RECORD_PUBLISHER_TYPE, conf.getConsumerRecordPublisherType());

        if (ConsumerConfigConstants.RecordPublisherType.EFO.name().equals(conf.getConsumerRecordPublisherType())) {
            kinesisProperties.setProperty(ConsumerConfigConstants.EFO_CONSUMER_NAME, conf.getConsumerEfoConsumerName());
        }

        return new FlinkKinesisConsumer(conf.getStreamName(), deserializationSchema, kinesisProperties);
    }

}
