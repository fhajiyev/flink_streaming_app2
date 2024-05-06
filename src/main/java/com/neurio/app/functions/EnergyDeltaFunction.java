package com.neurio.app.functions;

import com.neurio.app.common.utils.TimeUtils;
import com.neurio.app.dataframe.DataFrame;
import com.neurio.app.dataframe.component.EnergyDelta;
import com.neurio.app.dataframe.component.EnergyMetaData;
import com.neurio.app.dataframe.component.Raw;
import com.neurio.app.dto.LogEntryDto;
import com.neurio.app.protobuf.RecordSetProto;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

@Slf4j
public class EnergyDeltaFunction extends KeyedProcessFunction<String, Tuple2<String, RecordSetProto.RecordSet.Record>, DataFrame> {


    private transient ValueState<Tuple2<String, RecordSetProto.RecordSet.Record>> previousRecordState;

    @Override
    public void processElement(Tuple2<String, RecordSetProto.RecordSet.Record> tuple, Context ctx, Collector<DataFrame> collector) throws Exception {
        log.debug("Processing delta...");
        String hostRcpn = tuple.f0;
        RecordSetProto.RecordSet.Record currentRecord = tuple.f1;

        if (previousRecordState.value() == null) {
            previousRecordState.update(tuple);
            return;
        }

        RecordSetProto.RecordSet.Record previousRecord = previousRecordState.value().f1;

        long currentTimestamp = TimeUtils.roundDownToNearestTimestamp(currentRecord.getTimestamp(), 5, TimeUnit.MINUTES);
        long previousTimestamp = TimeUtils.roundDownToNearestTimestamp(previousRecord.getTimestamp(), 5, TimeUnit.MINUTES);

        if (isValidForProcessing(tuple, hostRcpn, currentTimestamp, previousTimestamp, ctx)) {
            previousRecordState.update(tuple);
            log.debug("updating state for device {} with event time {}", tuple.f0, currentTimestamp);

            Raw currentRaw = Raw.from(currentRecord);
            Raw previousRaw = Raw.from(previousRecord);
            EnergyDelta energyDelta = EnergyDelta.from(currentRaw, previousRaw);
            DataFrame df = DataFrame.builder()
                    .timestamp_utc(currentTimestamp - 1) // this is clock aligned
                    .energyMetaData(
                            EnergyMetaData.builder()
                                    .energyDelta(energyDelta)
                                    .currentRecord(currentRecord)
                                    .build()
                    )
                    .build();

            collector.collect(df);
        }


    }

    private boolean isValidForProcessing(Tuple2<String, RecordSetProto.RecordSet.Record> tuple, String hostRcpn, long currentTimestamp, long previousTimestamp, Context ctx) throws IOException {

        long expectedTimestamp = previousTimestamp + 300;

        if (currentTimestamp == previousTimestamp) {
            // we've processed this record already
            return false;
        }

        if (currentTimestamp < previousTimestamp) {
            // late event
            LogEntryDto logEntryDto = generateEventLog(hostRcpn, currentTimestamp, expectedTimestamp, "Late_Event");
            log.info(logEntryDto.toString());
            return false;

        }

        if (currentTimestamp > expectedTimestamp) {
            // future event
            previousRecordState.update(tuple);
            LogEntryDto logEntryDto = generateEventLog(hostRcpn, currentTimestamp, expectedTimestamp, "Future_Event");
            log.info(logEntryDto.toString());
            return false;
        }

        return true;
    }


    private LogEntryDto generateEventLog(String sensorId, long actualTimestamp, long expectedTimestamp, String eventType) {

        return LogEntryDto.builder()
                .identifier(sensorId)
                .eventType(eventType)
                .epochSeconds(Instant.now().getEpochSecond())
                .msg(String.format("[Processing time: %s] Was expecting %s timestamp, but got %s", Instant.now(), Instant.ofEpochSecond(expectedTimestamp), Instant.ofEpochSecond(actualTimestamp)))
                .build();
    }


    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Tuple2<String, RecordSetProto.RecordSet.Record>> recordStateDescriptor =
                new ValueStateDescriptor<>(
                        "previous record keyed by hostrcpn", // the state name
                        TypeInformation.of(new TypeHint<Tuple2<String, RecordSetProto.RecordSet.Record>>() {
                        }) // type information
                );

        this.previousRecordState = getRuntimeContext().getState(recordStateDescriptor);


    }
}
