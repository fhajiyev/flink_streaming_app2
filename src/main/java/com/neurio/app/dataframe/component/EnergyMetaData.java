package com.neurio.app.dataframe.component;

import com.neurio.app.protobuf.RecordSetProto;
import lombok.*;

import java.io.Serializable;

@Data
@Builder
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class EnergyMetaData implements Serializable {
    private static final long serialVersionUID = 2405172041950251807L;
    private RecordSetProto.RecordSet.Record currentRecord;
    private EnergyDelta energyDelta;
    private Raw raw;
    private Boolean isInverterOutputSwapped = false;

    public Raw getRaw() {
        if (raw == null) {
            raw = Raw.from(currentRecord);
        }
        return raw;
    }

}