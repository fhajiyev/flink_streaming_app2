package com.neurio.app.dataframe.component;

import com.neurio.app.protobuf.RecordSetProto;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.math.BigDecimal;
import java.text.MessageFormat;
import java.util.Objects;

@Data
@NoArgsConstructor
@Slf4j
@Builder
@AllArgsConstructor
@ToString
public final class EnergyDelta implements Serializable {
    private int time_delta_seconds;
    private Long inverter_energy_imported_delta_Ws;
    private Long inverter_energy_exported_delta_Ws;

    private EnergyDelta(Raw record, Raw previousRecord) {

        this.inverter_energy_exported_delta_Ws = calculateDifferenceOrDefaultNull(
                record.getRaw_inverter_lifetime_exported_Ws(),
                previousRecord.getRaw_inverter_lifetime_exported_Ws(),
                "raw_inverter_lifetime_exported_Ws");

        this.inverter_energy_imported_delta_Ws = calculateDifferenceOrDefaultNull(
                record.getRaw_inverter_lifetime_imported_Ws(),
                previousRecord.getRaw_inverter_lifetime_imported_Ws(),
                "raw_inverter_lifetime_imported_Ws");

        this.time_delta_seconds = (int) ((record.getEvent_timestamp()) - (previousRecord.getEvent_timestamp()));
    }

    public static EnergyDelta from(Raw currentRecord, Raw previousRecord) {
        EnergyDelta energyDelta = new EnergyDelta(currentRecord, previousRecord);
        return energyDelta;
    }

    public static EnergyDelta from(RecordSetProto.RecordSet.Record currentRecord, RecordSetProto.RecordSet.Record previousRecord) {
        return from(Raw.from(currentRecord), Raw.from(previousRecord));
    }

    private static Long calculateDifferenceOrDefaultNull(Long a, Long b, String valueDescription) {
        if (a == null && b == null) {
            return null;
        }
        if (a == null) {
            log.debug(
                    MessageFormat.format("{0} in current dataframe is not defined: some values may not be calculated.",
                            valueDescription));
            return null;
        }
        if (b == null) {
            log.debug(
                    MessageFormat.format("{0} in previous dataframe is not defined: some values may not be calculated.",
                            valueDescription));
            return null;
        }

        return a-b;
    }
}