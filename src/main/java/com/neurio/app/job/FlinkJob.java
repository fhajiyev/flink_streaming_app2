package com.neurio.app.job;

import com.neurio.app.config.AppConfig;
import com.neurio.app.dataframe.DataFrame;
import com.neurio.app.dataframe.DataframeKinesisPartitioner;
import com.neurio.app.dataframe.DataframeSerializationSchema;
import com.neurio.app.filter.RecordTimeRangeFilter;
import com.neurio.app.filter.RgmRecordValidationFilter;
import com.neurio.app.functions.AsyncSystemMetadataFetcherFunction;
import com.neurio.app.functions.EnergyDeltaFunction;
import com.neurio.app.functions.RgmCtInversionCorrectionFunction;
import com.neurio.app.partner.RgmTelemetryKinesisPartitioner;
import com.neurio.app.partner.RgmTelemetrySerializationSchema;
import com.neurio.app.protobuf.RecordSetProto;
import com.neurio.app.protobuf.utils.RecordSetExtractor;
import com.neurio.app.sink.KinesisSink;
import com.neurio.app.source.kinesis.KinesisSourceConsumer;
import com.neurio.app.source.kinesis.deserializer.RecordSetDeserializer;
import com.twitter.chill.protobuf.ProtobufSerializer;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

@Slf4j
@Getter
public class FlinkJob implements Runnable {

    private final StreamExecutionEnvironment env;

    public FlinkJob(AppConfig appConfig) {
        log.info("initializing flink job");
        this.env = StreamExecutionEnvironment.getExecutionEnvironment();

        if (appConfig.isLocalDevelopment()) {
            setUpLocalExecutionEnvironment();
        }

        env.getConfig().registerTypeWithKryoSerializer(RecordSetProto.RecordSet.class, ProtobufSerializer.class);
        env.getConfig().registerTypeWithKryoSerializer(RecordSetProto.RecordSet.Record.class, ProtobufSerializer.class);

        SingleOutputStreamOperator<DataFrame> stream = env
                .addSource(KinesisSourceConsumer.getKinesisSource(appConfig, new RecordSetDeserializer()))
                .uid("pika-rgm-kinesis-source")
                .name("Pika RGM Kinesis Source")

                .flatMap(new FlatMapFunction<RecordSetProto.RecordSet, Tuple2<String, RecordSetProto.RecordSet.Record>>() {
                     @Override
                     public void flatMap(RecordSetProto.RecordSet recordSet, Collector<Tuple2<String, RecordSetProto.RecordSet.Record>> collector) throws Exception {
                         for (RecordSetProto.RecordSet.Record record : recordSet.getRecordList()) {
                             // create a k,v pair of host rcpn and record
                             String hostRcpn = RecordSetExtractor.RecordExtractor.ChannelSampleExtractor.getHost_Rcpn(record.getChannelList(), "unknown");
                             if (hostRcpn.equals("unknown")) {
                                 log.info("Failed to find host rcpn for beacon {} at {}", recordSet.getSensorId(), (long) record.getTimestamp());
                                 return;
                             }
                             if (hostRcpn.isBlank()) {
                                 log.info("Empty host rcpn for beacon {} at {}", recordSet.getSensorId(), (long) record.getTimestamp());
                                 return;
                             }
                             collector.collect(new Tuple2<>(hostRcpn, record));
                         }
                     }

                })
                .name("recordset-to-record-flatmap")
                .uid("RecordSet Flatmap")

                .filter(new RecordTimeRangeFilter(appConfig.isLocalDevelopment()))
                .name("time-range-filter")
                .uid("Time Range Filter")

                .keyBy(tuple2 -> tuple2.f0) // keying by host rcpn
                .process(new EnergyDeltaFunction())
                .name("energy-delta-function")
                .uid("Energy Delta Function");

        DataStream<DataFrame> resultStream =
                AsyncDataStream.unorderedWait(stream, new AsyncSystemMetadataFetcherFunction(appConfig.getSystemServiceApiConfig(), appConfig.getDlqConfig(), appConfig.getNewRelicConfig(), appConfig.getGeneralConfig().getDeploymentType(), appConfig.isLocalDevelopment()), 10, TimeUnit.SECONDS, 50)
                .name("async-system-meta-data-fetcher")
                .uid("Async System Metadata Fetcher")
                .filter(new RgmRecordValidationFilter(appConfig.isLocalDevelopment()));

        DataStream<DataFrame> resultStreamRegistered =
                resultStream.filter(df -> df.getSystemMetaData().isRegisteredSystem());
        DataStream<DataFrame> resultStreamUnregistered =
                resultStream.filter(df -> !df.getSystemMetaData().isRegisteredSystem())
                        .map(df -> {
                            df.getEnergyMetaData().setIsInverterOutputSwapped(false);
                            return df;
                        });

        // only registered dataframes need to be corrected - ess-dp does not return records for unregistered systems
        DataStream<DataFrame> resultStreamWithCtInversionCorrected = resultStreamRegistered
                .keyBy(df -> df.getSystemMetaData().getSystemId()) // keying by system id
                .process(new RgmCtInversionCorrectionFunction(appConfig))
                .name("RGM Ct Inversion Correction Function")
                .uid("rgm-ct-inversion-correction-function");

        // unify corrected registered dataframes with unregistered dataframes
        resultStreamWithCtInversionCorrected = resultStreamWithCtInversionCorrected.union(resultStreamUnregistered);

        if(appConfig.getDataframeOutputStreamConfig().isKinesisSinkEnabled()) {
                resultStreamWithCtInversionCorrected
                .addSink(KinesisSink.getKinesisSink(
                        new DataframeSerializationSchema(),
                        new DataframeKinesisPartitioner(),
                        appConfig.getDataframeOutputStreamConfig().getKinesisEndpoint(),
                        appConfig.getDataframeOutputStreamConfig().getAwsRegion(),
                        appConfig.getDataframeOutputStreamConfig().getStreamName(),
                        appConfig.isLocalDevelopment()))
                .setParallelism(1)
                .name("dataframe-kinesis-sink")
                .uid("Dataframe Kinesis Sink");
        }

        if(appConfig.getPartnerRgmOutputStreamConfig().isKinesisSinkEnabled()) {
                resultStreamWithCtInversionCorrected
                .filter(df -> df.getSystemMetaData().isRegisteredSystem())
                .addSink(KinesisSink.getKinesisSink(
                        new RgmTelemetrySerializationSchema(),
                        new RgmTelemetryKinesisPartitioner(),
                        appConfig.getPartnerRgmOutputStreamConfig().getKinesisEndpoint(),
                        appConfig.getPartnerRgmOutputStreamConfig().getAwsRegion(),
                        appConfig.getPartnerRgmOutputStreamConfig().getStreamName(),
                        appConfig.isLocalDevelopment()))
                .setParallelism(1)
                .name("rgm-telemetry-kinesis-sink")
                .uid("RGM Telemetry Kinesis Sink");
        }

        //stream.addSink(new CollectSink());
    }

    @SneakyThrows
    @Override
    public void run() {
        log.info("starting...");
        try {
            env.execute("Ess device energy RGM processor");
        } catch (Exception e) {
            log.error("runner encountered exception {}...", e);
            throw e;
        }
    }


    private void setUpLocalExecutionEnvironment() {
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, Time.of(10, TimeUnit.SECONDS))); // look at how aws handles it
        env.setParallelism(2); //look at aws doc
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(Duration.ofMinutes(5).toSeconds());
    }

    // create a testing sink
    private static class CollectSink implements SinkFunction<RecordSetProto.RecordSet> {

        @Override
        public synchronized void invoke(RecordSetProto.RecordSet rs) {
            System.out.println("got recordset in sink");
        }
    }

}
