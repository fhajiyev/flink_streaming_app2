package com.neurio.app.job;


import com.generac.ces.EnergyDataframeProto;
import com.generac.ces.GeneracEventProtos;
import com.generac.ces.system.SystemModeOuterClass;
import com.generac.ces.telemetry.ChannelSampleOuterClass;
import com.generac.ces.telemetry.EnergyDeviceTypeOuterClass;
import com.generac.ces.telemetry.EnergyRecordOuterClass;
import com.generac.ces.telemetry.EnergyRecordSetOuterClass;
import com.google.protobuf.ByteString;
import com.neurio.app.integration.IntegrationTestBase;
import com.neurio.app.protobuf.RecordSetProto;
import com.neurio.app.protobuf.utils.RecordSetExtractor;
import com.neurio.app.testingutils.RecordSetProtoProvider;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;


@Slf4j
@Tag("IntegrationTest")
public class JobTest extends IntegrationTestBase {

    private static KinesisClient KINESIS_CLIENT = AWS_CONTAINER.getKinesisClient();

    @BeforeAll
    public static void setupJobTest() throws InterruptedException {
        //create source kinesis stream
        KINESIS_CLIENT.createStream(CreateStreamRequest.builder()
                .streamName(testAppConfig.getKinesisSourceConfig().getStreamName())
                .shardCount(2)
                .build());
        //create partner rgm kinesis sink stream
        KINESIS_CLIENT.createStream(CreateStreamRequest.builder()
                .streamName(testAppConfig.getPartnerRgmOutputStreamConfig().getStreamName())
                .shardCount(1)
                .build());
        //create dataframe kinesis sink stream
        KINESIS_CLIENT.createStream(CreateStreamRequest.builder()
                .streamName(testAppConfig.getDataframeOutputStreamConfig().getStreamName())
                .shardCount(1)
                .build());
        Thread.sleep(Duration.ofSeconds(2).toMillis());
    }

    @CsvSource({"src/test/resources/samples/recordsetproto/0001001200C1/2021-03-25;src/test/resources/samples/recordsetproto/0001001200E1/2021-03-25"})
    @ParameterizedTest
    @DisplayName("Test a job end to end using mocked apis")
    public void test(String folders) throws IOException, InterruptedException {

        String[] folderNames = folders.split(";");
        List<RecordSetProto.RecordSet> inputData1 = RecordSetProtoProvider.getRecordSetData(List.of(folderNames[0]));
        List<RecordSetProto.RecordSet> inputData2 = RecordSetProtoProvider.getRecordSetData(List.of(folderNames[1]));


        inputData1.stream()
                .map(recordSet -> ByteBuffer.wrap(recordSet.toByteArray()))
                .forEach(byteBuffer -> writeToStream(testAppConfig.getKinesisSourceConfig().getStreamName(), byteBuffer, inputData1.get(0).getSensorId()));
        inputData2.stream()
                .map(recordSet -> ByteBuffer.wrap(recordSet.toByteArray()))
                .forEach(byteBuffer -> writeToStream(testAppConfig.getKinesisSourceConfig().getStreamName(), byteBuffer, inputData2.get(0).getSensorId()));

        FlinkJob flinkJob = new FlinkJob(testAppConfig);

        ExecutorService jobExecutor = Executors.newCachedThreadPool();
        jobExecutor.submit(flinkJob);

        // let the job run and finish consuming from the stream
        jobExecutor.awaitTermination(1, TimeUnit.MINUTES);

        if (testAppConfig.getPartnerRgmOutputStreamConfig().isKinesisSinkEnabled()) {

            List<ByteBuffer> records = readFromStream(testAppConfig.getPartnerRgmOutputStreamConfig().getStreamName());
            assertThat(records.size(), equalTo(2));

            for(ByteBuffer bb : records) {

                GeneracEventProtos.GeneracEvent generacEvent = GeneracEventProtos.GeneracEvent.parseFrom(bb);

                // generac event assertions
                Assertions.assertTrue(generacEvent.hasEnergyRecordSet());

                // energy recordset assertions
                EnergyRecordSetOuterClass.EnergyRecordSet energyRecordSet = generacEvent.getEnergyRecordSet();
                assertThat(energyRecordSet.getEnergyRecordList().size(), equalTo(1));

                // energy record assertions
                EnergyRecordOuterClass.EnergyRecord energyRecord = energyRecordSet.getEnergyRecord(0);
                Assertions.assertEquals(energyRecord.getPreviousTimestamp(), 1.616544027E9);
                Assertions.assertEquals(energyRecord.getCurrentTimestamp(), 1.616544327E9);
                Assertions.assertEquals(energyRecord.getInverterState(), 2080); // GRID_CONNECTED
                Assertions.assertEquals(energyRecord.getSysMode(), SystemModeOuterClass.SystemMode.SELF_SUPPLY);
                Assertions.assertEquals(energyRecord.getChannelList().size(), 1);

                // channel assertions
                ChannelSampleOuterClass.ChannelSample channelSample = energyRecord.getChannel(0);
                Assertions.assertEquals(channelSample.getChannelType(), ChannelSampleOuterClass.ChannelType.INVERTER_OUTPUT);

                if(generacEvent.getSystemId().equals("systemId-000100073534")) {
                    Assertions.assertEquals(channelSample.getDeltaImportedEnergyWs(), 40481738496L - 40481590560L);
                    Assertions.assertEquals(channelSample.getDeltaExportedEnergyWs(), 1318551604L - 1318551904L);
                    Assertions.assertEquals(channelSample.getImportedEnergyWs(), 40481738496L);
                    Assertions.assertEquals(channelSample.getExportedEnergyWs(), 1318551604L);
                }
                else {
                    Assertions.assertEquals(channelSample.getDeltaImportedEnergyWs(), 1318551604L - 1318551904L);
                    Assertions.assertEquals(channelSample.getDeltaExportedEnergyWs(), 40481738496L - 40481590560L);
                    Assertions.assertEquals(channelSample.getImportedEnergyWs(), 1318551604L);
                    Assertions.assertEquals(channelSample.getExportedEnergyWs(), 40481738496L);
                }

                Assertions.assertEquals(channelSample.getDeviceType(), EnergyDeviceTypeOuterClass.EnergyDeviceType.INVERTER);
                Assertions.assertEquals(channelSample.getDeviceId(), generacEvent.getSystemId().substring("systemId-".length()));
                Assertions.assertEquals(channelSample.getDeviceIdBytes(), ByteString.copyFrom(generacEvent.getSystemId().substring("systemId-".length()).getBytes()));
                Assertions.assertEquals(channelSample.getRealPowerW(), 453);
                Assertions.assertEquals(channelSample.getChannelTypeValue(), ChannelSampleOuterClass.ChannelType.INVERTER_OUTPUT.getNumber());
                Assertions.assertEquals(channelSample.getDeviceTypeValue(), EnergyDeviceTypeOuterClass.EnergyDeviceType.INVERTER.getNumber());

            }

        }

        if (testAppConfig.getDataframeOutputStreamConfig().isKinesisSinkEnabled()) {

            List<ByteBuffer> records = readFromStream(testAppConfig.getDataframeOutputStreamConfig().getStreamName());
            assertThat(records.size(), equalTo(2));

            for(ByteBuffer bb : records) {

                EnergyDataframeProto.EnergyDataframe energyDataframe = EnergyDataframeProto.EnergyDataframe.parseFrom(bb);

                // dataframe assertions
                Assertions.assertEquals(energyDataframe.getTimestampUtc(), 1616544299L);
                Assertions.assertEquals(energyDataframe.getHostRcpn(), energyDataframe.getSystemId().substring("systemId-".length()));
                Assertions.assertEquals(energyDataframe.getDataframeType(), EnergyDataframeProto.DataframeType.RGM);
                Assertions.assertTrue(energyDataframe.hasRawData());
                Assertions.assertTrue(energyDataframe.hasDeltaData());
                Assertions.assertTrue(energyDataframe.hasRgmData());
                Assertions.assertFalse(energyDataframe.hasBillingData());
                Assertions.assertFalse(energyDataframe.hasAttributionData());

                // raw data assertions
                EnergyDataframeProto.RawData rawData = energyDataframe.getRawData();
                assertThat(1616544327L, equalTo(rawData.getRawTimestamp()));

                if(energyDataframe.getSystemId().equals("systemId-000100073534")) {
                    assertThat(40481738496L, equalTo(rawData.getRawInverterLifetimeImportedWs()));
                    assertThat(1318551604L, equalTo(rawData.getRawInverterLifetimeExportedWs()));
                } else {
                    assertThat(1318551604L, equalTo(rawData.getRawInverterLifetimeImportedWs()));
                    assertThat(40481738496L, equalTo(rawData.getRawInverterLifetimeExportedWs()));
                }

                // delta data assertions
                EnergyDataframeProto.DeltaData deltaData = energyDataframe.getDeltaData();
                assertThat(300, equalTo(deltaData.getTimeDiffInSeconds()));

                if(energyDataframe.getSystemId().equals("systemId-000100073534")) {
                    assertThat(40481738496L - 40481590560L, equalTo(deltaData.getInverterEnergyImportedWs()));
                    assertThat(1318551604L - 1318551904L, equalTo(deltaData.getInverterEnergyExportedWs()));
                } else {
                    assertThat(1318551604L - 1318551904L, equalTo(deltaData.getInverterEnergyImportedWs()));
                    assertThat(40481738496L - 40481590560L, equalTo(deltaData.getInverterEnergyExportedWs()));
                }

                // rgm data assertions
                EnergyDataframeProto.RgmData rgmData = energyDataframe.getRgmData();
                assertThat(453, equalTo(rgmData.getRealPowerW()));
                assertThat(1.2345f, equalTo(rgmData.getVoltageV()));
                assertThat(987, equalTo(rgmData.getReactivePowerVAR()));
                if(energyDataframe.getSystemId().equals("systemId-000100073534"))
                    assertThat(false, equalTo(rgmData.getIsInverterOutputSwapped()));
                else
                    assertThat(true, equalTo(rgmData.getIsInverterOutputSwapped()));

            }
        }
    }
}