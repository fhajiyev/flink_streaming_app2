package com.neurio.app.filter;

import com.neurio.app.protobuf.RecordSetProto;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.time.Duration;
import java.time.Instant;

public class RecordTimeRangeFilter implements FilterFunction<Tuple2<String, RecordSetProto.RecordSet.Record>> {

    private final boolean isLocalDevelopment;

    public RecordTimeRangeFilter(boolean isLocalDevelopment) {
        this.isLocalDevelopment = isLocalDevelopment;
    }

    @Override
    public boolean filter(Tuple2<String, RecordSetProto.RecordSet.Record> tuple2) {

        return validateTimestamp((long) tuple2.f1.getTimestamp());
    }

    // filters out records that are not within range of now - 7days < timestamp < now + 30 minutes
    // @return True for values that should be retained, false for values to be filtered out.
    private boolean validateTimestamp(long timestampSecond) {
        if (isLocalDevelopment) {
            return true;
        }
        long now = Instant.now().getEpochSecond();
        // system_energy_rgm/system_energy_rgm_unregistered both have a compression policy of 7 days
        boolean isOlderThan7Days = timestampSecond < (now - Duration.ofDays(7).toSeconds());
        boolean is30MinuteInTheFuture = timestampSecond > (now + Duration.ofMinutes(30).toSeconds());
        return !is30MinuteInTheFuture && !isOlderThan7Days;
    }
}
