package com.neurio.app.dataframe;

import com.neurio.app.dataframe.component.EnergyMetaData;
import com.neurio.app.dataframe.component.SystemMetaData;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;

@Slf4j
@Builder
@ToString
@Data
@NoArgsConstructor
@AllArgsConstructor
public class DataFrame implements Serializable {
    private static final long serialVersionUID = 2405172041950251807L;
    // this is clock aligned timestamp in seconds

    private long timestamp_utc;
    private EnergyMetaData energyMetaData;
    private SystemMetaData systemMetaData;
}
