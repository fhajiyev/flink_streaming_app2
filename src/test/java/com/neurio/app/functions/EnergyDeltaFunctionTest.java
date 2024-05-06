package com.neurio.app.functions;


import com.neurio.app.dataframe.DataFrame;
import com.neurio.app.protobuf.RecordSetProto;
import com.neurio.app.testingutils.RecordSetProtoProvider;
import com.twitter.chill.protobuf.ProtobufSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.List;

@Slf4j
@Tag("UnitTest")
class EnergyDeltaFunctionTest {


    @CsvSource({"src/test/resources/samples/recordsetproto/0001001200C1/2021-03-24"})
    @ParameterizedTest
    public void testSystemResponseTTL(String folder) throws Exception {

        EnergyDeltaFunction classUnderTest = new EnergyDeltaFunction();

        KeyedOneInputStreamOperatorTestHarness<String, Tuple2<String, RecordSetProto.RecordSet.Record>, DataFrame> harness = ProcessFunctionTestHarnesses
                .forKeyedProcessFunction(
                        classUnderTest, (tuple2) -> tuple2.f0, Types.STRING);

        harness.getExecutionConfig().registerTypeWithKryoSerializer(RecordSetProto.RecordSet.Record.class, ProtobufSerializer.class);

        List<RecordSetProto.RecordSet> sourceData = RecordSetProtoProvider.getRecordSetData(List.of(folder));


        for (int i = 0; i < sourceData.size(); i++) {
            try {
                harness.processElement(new StreamRecord<Tuple2<String, RecordSetProto.RecordSet.Record>>(new Tuple2(sourceData.get(i).getSensorId(), sourceData.get(i).getRecord(0))));
            } catch (Exception e) {
                Assertions.fail();
            }
        }

        List<DataFrame> outputCollection = harness.extractOutputValues();
        Assertions.assertEquals(sourceData.size() -1, outputCollection.size());

        outputCollection.forEach(df -> {
            Assertions.assertNotNull(df.getEnergyMetaData());
            Assertions.assertNotNull(df.getEnergyMetaData().getEnergyDelta());
            Assertions.assertNotNull(df.getEnergyMetaData().getRaw());
        });

    }

}
