package com.neurio.app.testingutils;

import com.neurio.app.common.utils.TimeUtils;
import com.neurio.app.dataframe.DataFrame;
import com.neurio.app.dataframe.component.EnergyDelta;
import com.neurio.app.dataframe.component.EnergyMetaData;
import com.neurio.app.protobuf.RecordSetProto;
import com.neurio.app.protobuf.utils.RecordSetExtractor;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class DataFrameProvider {

    public static List<DataFrame> getDataFrames(List<RecordSetProto.RecordSet> recordSets, String timezone, boolean isRegistered) {
        List<DataFrame> list = new ArrayList<>();
        long nonce = UUID.randomUUID().getLeastSignificantBits();
        for (int i = 1; i < recordSets.size(); i++) {
            RecordSetProto.RecordSet.Record previousRecord = recordSets.get(i - 1).getRecord(0);
            RecordSetProto.RecordSet.Record currentRecord = recordSets.get(i).getRecord(0);

            EnergyDelta energyDelta = EnergyDelta.from(currentRecord, previousRecord);
            long now = TimeUtils.getNearestTimeStamp((long) currentRecord.getTimestamp(), 5, TimeUnit.MINUTES) - 1;
            String hostRcpn = RecordSetExtractor.RecordExtractor.ChannelSampleExtractor.getHost_Rcpn(currentRecord.getChannelList(), "unknown");
            com.neurio.app.dataframe.DataFrame dataFrame = com.neurio.app.dataframe.DataFrame.builder()
                    .systemMetaData(com.neurio.app.dataframe.component.SystemMetaData.builder()
                            .timezone(timezone)
                            .systemId("system-" + hostRcpn + "-" + nonce)
                            .hostRcpn("hostrcpn-" + hostRcpn)
                            .registeredSystem(isRegistered)
                            .build()
                    )
                    .timestamp_utc(now)
                    .energyMetaData(EnergyMetaData.builder()
                            .currentRecord(currentRecord)
                            .energyDelta(energyDelta)
                            .build()
                    )
                    .build();
            list.add(dataFrame);
        }
        return list;
    }
}
