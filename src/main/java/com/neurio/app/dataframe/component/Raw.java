package com.neurio.app.dataframe.component;

import com.neurio.app.protobuf.RecordSetProto;
import com.neurio.app.protobuf.utils.RecordMapper;
import com.neurio.app.protobuf.utils.RecordSetExtractor;
import lombok.*;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Map;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class Raw implements Serializable {
    private Long event_timestamp;
    private Long raw_inverter_lifetime_imported_Ws;
    private Long raw_inverter_lifetime_exported_Ws;
    private String host_rcpn;

    // RGM-specific parameters
    private Integer realPower_W;
    private Float voltage_V;
    private Integer reactivePower_VAR;

    private Raw(RecordSetProto.RecordSet.Record record) {
        Map recordMap = RecordMapper.toRecordMap(record);
        this.event_timestamp = (long) record.getTimestamp();
        this.raw_inverter_lifetime_exported_Ws = RecordSetExtractor.RecordExtractor.ChannelSampleExtractor.getRaw_inverter_lifetime_exported_Ws(recordMap, null);
        this.raw_inverter_lifetime_imported_Ws = RecordSetExtractor.RecordExtractor.ChannelSampleExtractor.getRaw_inverter_lifetime_imported_or_default_Ws(recordMap, null);
        this.host_rcpn = RecordSetExtractor.RecordExtractor.ChannelSampleExtractor.getHost_Rcpn(recordMap, null);
        this.realPower_W = RecordSetExtractor.RecordExtractor.ChannelSampleExtractor.getRealPower_W(recordMap, null);
        this.voltage_V = RecordSetExtractor.RecordExtractor.ChannelSampleExtractor.getVoltage_V(recordMap, null);
        this.reactivePower_VAR = RecordSetExtractor.RecordExtractor.ChannelSampleExtractor.getReactivePower_VAR(recordMap, null);
    }


    public static Raw from(@NonNull RecordSetProto.RecordSet.Record record) {
        return new Raw(record);
    }
}
