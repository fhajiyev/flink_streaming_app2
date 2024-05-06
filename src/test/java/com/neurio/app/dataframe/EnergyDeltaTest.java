package com.neurio.app.dataframe;

import com.neurio.app.dataframe.component.EnergyDelta;
import com.neurio.app.protobuf.RecordSetProto;
import com.neurio.app.protobuf.utils.RecordMapper;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import com.neurio.app.testingutils.RecordSetProtoProvider;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

@Slf4j
@Tag("UnitTest")
public class EnergyDeltaTest {


    @CsvSource({
            "src/test/resources/samples/recordsetproto/0001001200C1/2021-03-24"
    })
    @ParameterizedTest()
    public void testEnergyDelta(String folderPath) throws IOException {

        List<RecordSetProto.RecordSet> recordSets = RecordSetProtoProvider.getRecordSetData(List.of(folderPath));

        for (int i = 1; i < recordSets.size(); i++) {
            RecordSetProto.RecordSet.Record currentRecord = recordSets.get(i).getRecord(0);
            RecordSetProto.RecordSet.Record previousRecord = recordSets.get(i - 1).getRecord(0);
            EnergyDelta energyDelta = EnergyDelta.from(currentRecord, previousRecord);

            Map<RecordSetProto.RecordSet.Record.ChannelSample.ChannelType, RecordSetProto.RecordSet.Record.ChannelSample> currentRecordMap = RecordMapper.toRecordMap(currentRecord);
            Map<RecordSetProto.RecordSet.Record.ChannelSample.ChannelType, RecordSetProto.RecordSet.Record.ChannelSample> previousRecordMap = RecordMapper.toRecordMap(previousRecord);

            Assertions.assertTrue(currentRecordMap.size() == previousRecordMap.size());

            currentRecordMap.forEach((channelType, channelSample) -> {
                var deltaImport = channelSample.hasImportedEnergyWs() ? channelSample.getImportedEnergyWs() - previousRecordMap.get(channelType).getImportedEnergyWs() : null;
                var deltaExport = channelSample.hasExportedEnergyWs() ? channelSample.getExportedEnergyWs() - previousRecordMap.get(channelType).getExportedEnergyWs() : null;

                if (RecordSetProto.RecordSet.Record.ChannelSample.ChannelType.INVERTER_OUTPUT.equals(channelType)) {
                    Assertions.assertEquals(deltaImport, energyDelta.getInverter_energy_exported_delta_Ws());
                    Assertions.assertEquals(deltaExport, energyDelta.getInverter_energy_imported_delta_Ws());
                }
            });
            for (Map.Entry<RecordSetProto.RecordSet.Record.ChannelSample.ChannelType, RecordSetProto.RecordSet.Record.ChannelSample> entry : currentRecordMap.entrySet()) {

            }

            Assertions.assertEquals(currentRecord.getTimestamp() - previousRecord.getTimestamp(), energyDelta.getTime_delta_seconds());
        }

    }

}
