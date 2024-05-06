package com.neurio.app.functions;

import com.neurio.app.api.services.essdp.EssDpServiceStub;
import com.neurio.app.api.services.essdp.IEssDpService;
import com.neurio.app.common.utils.TimeUtils;
import com.neurio.app.config.AppConfig;
import com.neurio.app.dataframe.DataFrame;
import com.neurio.app.dataframe.component.EnergyDelta;
import com.neurio.app.dataframe.component.EnergyMetaData;
import com.neurio.app.dataframe.component.Raw;
import com.neurio.app.dataframe.component.SystemMetaData;
import com.neurio.app.protobuf.RecordSetProto;
import com.neurio.app.testingutils.RecordSetProtoProvider;
import com.twitter.chill.protobuf.ProtobufSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
@Tag("UnitTest")
@ExtendWith(MockitoExtension.class)
class RgmCtInversionCorrectionFunctionTest {

    @Spy
    private EssDpServiceStub essDpServiceStub = new EssDpServiceStub();

    @CsvSource({"src/test/resources/samples/recordsetproto/0001001200C1/2021-03-24"})
    @ParameterizedTest
    public void testSwapped(String folder) throws Exception {
        AppConfig appConfig = new AppConfig();
        appConfig.setEssDpServiceApiConfig(AppConfig.EssDpServiceApiConfig.builder()
                .useStub(true)
                .build());

        RgmCtInversionCorrectionFunction classUnderTest = new RgmCtInversionCorrectionFunction(appConfig);
        classUnderTest.setEssCheckIntervalHours(1);

        // set dataframes to be 30 min apart, hence with 1 hour threshold, ess-dp will be called 4 times
        Number[] dfTimestamps = {
                1.616545827E9, // ess-dp called (no state yet)
                1.616547627E9,
                1.616549427E9, // ess-dp called
                1.616551227E9,
                1.616553027E9, // ess-dp called
                1.616554827E9,
                1.616556627E9, // ess-dp called
                1.616558427E9
        };

        Method pMethod = RgmCtInversionCorrectionFunction.class.getDeclaredMethod("setEssDpService", IEssDpService.class);
        pMethod.setAccessible(true);
        pMethod.invoke(classUnderTest, essDpServiceStub);

        KeyedOneInputStreamOperatorTestHarness<String, DataFrame, DataFrame> harness = ProcessFunctionTestHarnesses
                .forKeyedProcessFunction(
                        classUnderTest, df -> df.getSystemMetaData().getSystemId(), Types.STRING);

        harness.getExecutionConfig().registerTypeWithKryoSerializer(RecordSetProto.RecordSet.Record.class, ProtobufSerializer.class);

        List<DataFrame> dataFrames = new ArrayList<>();
        List<DataFrame> dataFramesBackup = new ArrayList<>();
        List<RecordSetProto.RecordSet> inputData = RecordSetProtoProvider.getRecordSetData(List.of(folder));
        for (int i = 1; i < inputData.size(); i++) {
            RecordSetProto.RecordSet.Record currentRecord = inputData.get(i).getRecord(0);
            RecordSetProto.RecordSet.Record previousRecord = inputData.get(i-1).getRecord(0);

            long currentTimestamp = TimeUtils.roundDownToNearestTimestamp(dfTimestamps[i-1], 5, TimeUnit.MINUTES);

            Raw currentRaw = Raw.from(currentRecord);
            Raw previousRaw = Raw.from(previousRecord);
            EnergyDelta energyDelta = EnergyDelta.from(currentRaw, previousRaw);
            DataFrame df = DataFrame.builder()
                    .timestamp_utc(currentTimestamp - 1)
                    .energyMetaData(
                            EnergyMetaData.builder()
                                    .energyDelta(energyDelta)
                                    .currentRecord(currentRecord)
                                    .build()
                    )
                    .systemMetaData(
                            SystemMetaData.builder()
                                    .systemId("systemId-000100093534")
                                    .build()
                    )
                    .build();

            // create backup dataframe to compare output with, because original dataframe will be modified
            Raw currentRawBackup = Raw.from(currentRecord);
            Raw previousRawBackup = Raw.from(previousRecord);
            EnergyDelta energyDeltaBackup = EnergyDelta.from(currentRawBackup, previousRawBackup);
            DataFrame dfBackup = DataFrame.builder()
                    .timestamp_utc(currentTimestamp - 1)
                    .energyMetaData(
                            EnergyMetaData.builder()
                                    .energyDelta(energyDeltaBackup)
                                    .currentRecord(currentRecord)
                                    .build()
                    )
                    .systemMetaData(
                            SystemMetaData.builder()
                                    .systemId("systemId-000100093534")
                                    .build()
                    )
                    .build();

            dataFrames.add(df);
            dataFramesBackup.add(dfBackup);
        }

        for (int i = 0; i < dataFrames.size(); i++) {
            try {
                harness.processElement(new StreamRecord<DataFrame>(dataFrames.get(i)));
            } catch (Exception e) {
                Assertions.fail();
            }
        }

        List<DataFrame> outputCollection = harness.extractOutputValues();
        Assertions.assertEquals(dataFrames.size(), inputData.size()-1);
        Assertions.assertEquals(dataFrames.size(), outputCollection.size());

        Mockito.verify(essDpServiceStub, Mockito.times(4)).getEssEnergyByStartAndEndTimes(Mockito.anyString(), Mockito.anyLong(), Mockito.anyLong());

        List<DataFrame> output = harness.extractOutputValues();
        for(int i = 0; i < output.size(); i++) {

            // swap flag
            Assertions.assertEquals(output.get(i).getEnergyMetaData().getIsInverterOutputSwapped(), true);
            // deltas
            Assertions.assertEquals(output.get(i).getEnergyMetaData().getEnergyDelta().getInverter_energy_imported_delta_Ws(),
                    dataFramesBackup.get(i).getEnergyMetaData().getEnergyDelta().getInverter_energy_exported_delta_Ws());
            Assertions.assertEquals(output.get(i).getEnergyMetaData().getEnergyDelta().getInverter_energy_exported_delta_Ws(),
                    dataFramesBackup.get(i).getEnergyMetaData().getEnergyDelta().getInverter_energy_imported_delta_Ws());
            // raws
            Assertions.assertEquals(output.get(i).getEnergyMetaData().getRaw().getRaw_inverter_lifetime_imported_Ws(),
                    dataFramesBackup.get(i).getEnergyMetaData().getRaw().getRaw_inverter_lifetime_exported_Ws());
            Assertions.assertEquals(output.get(i).getEnergyMetaData().getRaw().getRaw_inverter_lifetime_exported_Ws(),
                    dataFramesBackup.get(i).getEnergyMetaData().getRaw().getRaw_inverter_lifetime_imported_Ws());

        }
    }

    @CsvSource({"src/test/resources/samples/recordsetproto/0001001200E1/2021-03-24"})
    @ParameterizedTest
    public void testNotSwapped(String folder) throws Exception {
        AppConfig appConfig = new AppConfig();
        appConfig.setEssDpServiceApiConfig(AppConfig.EssDpServiceApiConfig.builder()
                .useStub(true)
                .build());

        RgmCtInversionCorrectionFunction classUnderTest = new RgmCtInversionCorrectionFunction(appConfig);
        classUnderTest.setEssCheckIntervalHours(1);

        // set dataframes to be 30 min apart, hence with 1 hour threshold, ess-dp will be called 4 times
        Number[] dfTimestamps = {
                1.616545827E9, // ess-dp called (no state yet)
                1.616547627E9,
                1.616549427E9, // ess-dp called
                1.616551227E9,
                1.616553027E9, // ess-dp called
                1.616554827E9,
                1.616556627E9, // ess-dp called
                1.616558427E9
        };

        Method pMethod = RgmCtInversionCorrectionFunction.class.getDeclaredMethod("setEssDpService", IEssDpService.class);
        pMethod.setAccessible(true);
        pMethod.invoke(classUnderTest, essDpServiceStub);

        KeyedOneInputStreamOperatorTestHarness<String, DataFrame, DataFrame> harness = ProcessFunctionTestHarnesses
                .forKeyedProcessFunction(
                        classUnderTest, df -> df.getSystemMetaData().getSystemId(), Types.STRING);

        harness.getExecutionConfig().registerTypeWithKryoSerializer(RecordSetProto.RecordSet.Record.class, ProtobufSerializer.class);

        List<DataFrame> dataFrames = new ArrayList<>();
        List<DataFrame> dataFramesBackup = new ArrayList<>();
        List<RecordSetProto.RecordSet> inputData = RecordSetProtoProvider.getRecordSetData(List.of(folder));
        for (int i = 1; i < inputData.size(); i++) {
            RecordSetProto.RecordSet.Record currentRecord = inputData.get(i).getRecord(0);
            RecordSetProto.RecordSet.Record previousRecord = inputData.get(i-1).getRecord(0);

            long currentTimestamp = TimeUtils.roundDownToNearestTimestamp(dfTimestamps[i-1], 5, TimeUnit.MINUTES);

            Raw currentRaw = Raw.from(currentRecord);
            Raw previousRaw = Raw.from(previousRecord);
            EnergyDelta energyDelta = EnergyDelta.from(currentRaw, previousRaw);
            DataFrame df = DataFrame.builder()
                    .timestamp_utc(currentTimestamp - 1)
                    .energyMetaData(
                            EnergyMetaData.builder()
                                    .energyDelta(energyDelta)
                                    .currentRecord(currentRecord)
                                    .build()
                    )
                    .systemMetaData(
                            SystemMetaData.builder()
                                    .systemId("systemId-000100073534")
                                    .build()
                    )
                    .build();

            // create backup dataframe to compare output with, because original dataframe will be modified
            Raw currentRawBackup = Raw.from(currentRecord);
            Raw previousRawBackup = Raw.from(previousRecord);
            EnergyDelta energyDeltaBackup = EnergyDelta.from(currentRawBackup, previousRawBackup);
            DataFrame dfBackup = DataFrame.builder()
                    .timestamp_utc(currentTimestamp - 1)
                    .energyMetaData(
                            EnergyMetaData.builder()
                                    .energyDelta(energyDeltaBackup)
                                    .currentRecord(currentRecord)
                                    .build()
                    )
                    .systemMetaData(
                            SystemMetaData.builder()
                                    .systemId("systemId-000100073534")
                                    .build()
                    )
                    .build();

            dataFrames.add(df);
            dataFramesBackup.add(dfBackup);
        }

        for (int i = 0; i < dataFrames.size(); i++) {
            try {
                harness.processElement(new StreamRecord<DataFrame>(dataFrames.get(i)));
            } catch (Exception e) {
                Assertions.fail();
            }
        }

        List<DataFrame> outputCollection = harness.extractOutputValues();
        Assertions.assertEquals(dataFrames.size(), inputData.size()-1);
        Assertions.assertEquals(dataFrames.size(), outputCollection.size());

        Mockito.verify(essDpServiceStub, Mockito.times(4)).getEssEnergyByStartAndEndTimes(Mockito.anyString(), Mockito.anyLong(), Mockito.anyLong());

        List<DataFrame> output = harness.extractOutputValues();
        for(int i = 0; i < output.size(); i++) {

            // swap flag
            Assertions.assertEquals(output.get(i).getEnergyMetaData().getIsInverterOutputSwapped(), false);
            // deltas
            Assertions.assertEquals(output.get(i).getEnergyMetaData().getEnergyDelta().getInverter_energy_imported_delta_Ws(),
                    dataFramesBackup.get(i).getEnergyMetaData().getEnergyDelta().getInverter_energy_imported_delta_Ws());
            Assertions.assertEquals(output.get(i).getEnergyMetaData().getEnergyDelta().getInverter_energy_exported_delta_Ws(),
                    dataFramesBackup.get(i).getEnergyMetaData().getEnergyDelta().getInverter_energy_exported_delta_Ws());
            // raws
            Assertions.assertEquals(output.get(i).getEnergyMetaData().getRaw().getRaw_inverter_lifetime_imported_Ws(),
                    dataFramesBackup.get(i).getEnergyMetaData().getRaw().getRaw_inverter_lifetime_imported_Ws());
            Assertions.assertEquals(output.get(i).getEnergyMetaData().getRaw().getRaw_inverter_lifetime_exported_Ws(),
                    dataFramesBackup.get(i).getEnergyMetaData().getRaw().getRaw_inverter_lifetime_exported_Ws());

        }
    }

    @CsvSource({"src/test/resources/samples/recordsetproto/0001001200F1/2021-03-24"})
    @ParameterizedTest
    public void testRgmSameImpExpDeltas(String folder) throws Exception {
        AppConfig appConfig = new AppConfig();
        appConfig.setEssDpServiceApiConfig(AppConfig.EssDpServiceApiConfig.builder()
                .useStub(true)
                .build());

        RgmCtInversionCorrectionFunction classUnderTest = new RgmCtInversionCorrectionFunction(appConfig);
        classUnderTest.setEssCheckIntervalHours(1);

        Method pMethod = RgmCtInversionCorrectionFunction.class.getDeclaredMethod("setEssDpService", IEssDpService.class);
        pMethod.setAccessible(true);
        pMethod.invoke(classUnderTest, essDpServiceStub);

        KeyedOneInputStreamOperatorTestHarness<String, DataFrame, DataFrame> harness = ProcessFunctionTestHarnesses
                .forKeyedProcessFunction(
                        classUnderTest, df -> df.getSystemMetaData().getSystemId(), Types.STRING);

        harness.getExecutionConfig().registerTypeWithKryoSerializer(RecordSetProto.RecordSet.Record.class, ProtobufSerializer.class);

        List<DataFrame> dataFrames = new ArrayList<>();
        List<DataFrame> dataFramesBackup = new ArrayList<>();
        List<RecordSetProto.RecordSet> inputData = RecordSetProtoProvider.getRecordSetData(List.of(folder));
        for (int i = 1; i < inputData.size(); i++) {
            RecordSetProto.RecordSet.Record currentRecord = inputData.get(i).getRecord(0);
            RecordSetProto.RecordSet.Record previousRecord = inputData.get(i-1).getRecord(0);

            long currentTimestamp = TimeUtils.roundDownToNearestTimestamp(currentRecord.getTimestamp(), 5, TimeUnit.MINUTES);

            Raw currentRaw = Raw.from(currentRecord);
            Raw previousRaw = Raw.from(previousRecord);
            EnergyDelta energyDelta = EnergyDelta.from(currentRaw, previousRaw);
            DataFrame df = DataFrame.builder()
                    .timestamp_utc(currentTimestamp - 1)
                    .energyMetaData(
                            EnergyMetaData.builder()
                                    .energyDelta(energyDelta)
                                    .currentRecord(currentRecord)
                                    .build()
                    )
                    .systemMetaData(
                            SystemMetaData.builder()
                                    .systemId("systemId-000100053534")
                                    .build()
                    )
                    .build();

            // create backup dataframe to compare output with, because original dataframe will be modified
            Raw currentRawBackup = Raw.from(currentRecord);
            Raw previousRawBackup = Raw.from(previousRecord);
            EnergyDelta energyDeltaBackup = EnergyDelta.from(currentRawBackup, previousRawBackup);
            DataFrame dfBackup = DataFrame.builder()
                    .timestamp_utc(currentTimestamp - 1)
                    .energyMetaData(
                            EnergyMetaData.builder()
                                    .energyDelta(energyDeltaBackup)
                                    .currentRecord(currentRecord)
                                    .build()
                    )
                    .systemMetaData(
                            SystemMetaData.builder()
                                    .systemId("systemId-000100053534")
                                    .build()
                    )
                    .build();

            dataFrames.add(df);
            dataFramesBackup.add(dfBackup);
        }

        for (int i = 0; i < dataFrames.size(); i++) {
            try {
                harness.processElement(new StreamRecord<DataFrame>(dataFrames.get(i)));
            } catch (Exception e) {
                Assertions.fail();
            }
        }

        List<DataFrame> outputCollection = harness.extractOutputValues();
        Assertions.assertEquals(dataFrames.size(), inputData.size()-1);
        Assertions.assertEquals(dataFrames.size(), outputCollection.size());

        // no state stored
        Assertions.assertEquals(harness.numKeyedStateEntries(), 0);

        // ess-dp will not be called
        Mockito.verify(essDpServiceStub, Mockito.times(0)).getEssEnergyByStartAndEndTimes(Mockito.anyString(), Mockito.anyLong(), Mockito.anyLong());

        List<DataFrame> output = harness.extractOutputValues();
        for(int i = 0; i < output.size(); i++) {

            // swap flag
            Assertions.assertEquals(output.get(i).getEnergyMetaData().getIsInverterOutputSwapped(), false);
            // deltas
            Assertions.assertEquals(output.get(i).getEnergyMetaData().getEnergyDelta().getInverter_energy_imported_delta_Ws(),
                    dataFramesBackup.get(i).getEnergyMetaData().getEnergyDelta().getInverter_energy_imported_delta_Ws());
            Assertions.assertEquals(output.get(i).getEnergyMetaData().getEnergyDelta().getInverter_energy_exported_delta_Ws(),
                    dataFramesBackup.get(i).getEnergyMetaData().getEnergyDelta().getInverter_energy_exported_delta_Ws());
            // raws
            Assertions.assertEquals(output.get(i).getEnergyMetaData().getRaw().getRaw_inverter_lifetime_imported_Ws(),
                    dataFramesBackup.get(i).getEnergyMetaData().getRaw().getRaw_inverter_lifetime_imported_Ws());
            Assertions.assertEquals(output.get(i).getEnergyMetaData().getRaw().getRaw_inverter_lifetime_exported_Ws(),
                    dataFramesBackup.get(i).getEnergyMetaData().getRaw().getRaw_inverter_lifetime_exported_Ws());

        }
    }

    @CsvSource({"src/test/resources/samples/recordsetproto/0001001200F1/2021-03-25"})
    @ParameterizedTest
    public void testEssSameImpExpDeltas(String folder) throws Exception {
        AppConfig appConfig = new AppConfig();
        appConfig.setEssDpServiceApiConfig(AppConfig.EssDpServiceApiConfig.builder()
                .useStub(true)
                .build());

        RgmCtInversionCorrectionFunction classUnderTest = new RgmCtInversionCorrectionFunction(appConfig);
        classUnderTest.setEssCheckIntervalHours(1);

        Method pMethod = RgmCtInversionCorrectionFunction.class.getDeclaredMethod("setEssDpService", IEssDpService.class);
        pMethod.setAccessible(true);
        pMethod.invoke(classUnderTest, essDpServiceStub);

        KeyedOneInputStreamOperatorTestHarness<String, DataFrame, DataFrame> harness = ProcessFunctionTestHarnesses
                .forKeyedProcessFunction(
                        classUnderTest, df -> df.getSystemMetaData().getSystemId(), Types.STRING);

        harness.getExecutionConfig().registerTypeWithKryoSerializer(RecordSetProto.RecordSet.Record.class, ProtobufSerializer.class);

        List<DataFrame> dataFrames = new ArrayList<>();
        List<DataFrame> dataFramesBackup = new ArrayList<>();
        List<RecordSetProto.RecordSet> inputData = RecordSetProtoProvider.getRecordSetData(List.of(folder));
        for (int i = 1; i < inputData.size(); i++) {
            RecordSetProto.RecordSet.Record currentRecord = inputData.get(i).getRecord(0);
            RecordSetProto.RecordSet.Record previousRecord = inputData.get(i-1).getRecord(0);

            long currentTimestamp = TimeUtils.roundDownToNearestTimestamp(currentRecord.getTimestamp(), 5, TimeUnit.MINUTES);

            Raw currentRaw = Raw.from(currentRecord);
            Raw previousRaw = Raw.from(previousRecord);
            EnergyDelta energyDelta = EnergyDelta.from(currentRaw, previousRaw);
            DataFrame df = DataFrame.builder()
                    .timestamp_utc(currentTimestamp - 1)
                    .energyMetaData(
                            EnergyMetaData.builder()
                                    .energyDelta(energyDelta)
                                    .currentRecord(currentRecord)
                                    .build()
                    )
                    .systemMetaData(
                            SystemMetaData.builder()
                                    .systemId("systemId-000100053534")
                                    .build()
                    )
                    .build();

            // create backup dataframe to compare output with, because original dataframe will be modified
            Raw currentRawBackup = Raw.from(currentRecord);
            Raw previousRawBackup = Raw.from(previousRecord);
            EnergyDelta energyDeltaBackup = EnergyDelta.from(currentRawBackup, previousRawBackup);
            DataFrame dfBackup = DataFrame.builder()
                    .timestamp_utc(currentTimestamp - 1)
                    .energyMetaData(
                            EnergyMetaData.builder()
                                    .energyDelta(energyDeltaBackup)
                                    .currentRecord(currentRecord)
                                    .build()
                    )
                    .systemMetaData(
                            SystemMetaData.builder()
                                    .systemId("systemId-000100053534")
                                    .build()
                    )
                    .build();

            dataFrames.add(df);
            dataFramesBackup.add(dfBackup);
        }

        for (int i = 0; i < dataFrames.size(); i++) {
            try {
                harness.processElement(new StreamRecord<DataFrame>(dataFrames.get(i)));
            } catch (Exception e) {
                Assertions.fail();
            }
        }

        List<DataFrame> outputCollection = harness.extractOutputValues();
        Assertions.assertEquals(dataFrames.size(), inputData.size()-1);
        Assertions.assertEquals(dataFrames.size(), outputCollection.size());

        // no state stored
        Assertions.assertEquals(harness.numKeyedStateEntries(), 0);

        // ess-dp will be called once
        Mockito.verify(essDpServiceStub, Mockito.times(1)).getEssEnergyByStartAndEndTimes(Mockito.anyString(), Mockito.anyLong(), Mockito.anyLong());

        List<DataFrame> output = harness.extractOutputValues();
        for(int i = 0; i < output.size(); i++) {

            // swap flag
            Assertions.assertEquals(output.get(i).getEnergyMetaData().getIsInverterOutputSwapped(), false);
            // deltas
            Assertions.assertEquals(output.get(i).getEnergyMetaData().getEnergyDelta().getInverter_energy_imported_delta_Ws(),
                    dataFramesBackup.get(i).getEnergyMetaData().getEnergyDelta().getInverter_energy_imported_delta_Ws());
            Assertions.assertEquals(output.get(i).getEnergyMetaData().getEnergyDelta().getInverter_energy_exported_delta_Ws(),
                    dataFramesBackup.get(i).getEnergyMetaData().getEnergyDelta().getInverter_energy_exported_delta_Ws());
            // raws
            Assertions.assertEquals(output.get(i).getEnergyMetaData().getRaw().getRaw_inverter_lifetime_imported_Ws(),
                    dataFramesBackup.get(i).getEnergyMetaData().getRaw().getRaw_inverter_lifetime_imported_Ws());
            Assertions.assertEquals(output.get(i).getEnergyMetaData().getRaw().getRaw_inverter_lifetime_exported_Ws(),
                    dataFramesBackup.get(i).getEnergyMetaData().getRaw().getRaw_inverter_lifetime_exported_Ws());

        }
    }
}
