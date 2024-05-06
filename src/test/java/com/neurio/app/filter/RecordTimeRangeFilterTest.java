package com.neurio.app.filter;


import com.neurio.app.protobuf.RecordSetProto;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@Tag("UnitTest")
public class RecordTimeRangeFilterTest {

    static Stream<Arguments> dataSetProvider() {
        return Stream.of(
                Arguments.of(
                        new TestDataSet(
                                Instant.now().getEpochSecond() - Duration.ofDays(90).toSeconds(),
                                false)
                ),
                Arguments.of(
                        new TestDataSet(
                                Instant.now().getEpochSecond() - Duration.ofDays(7).toSeconds() + 60,// testing edge cases, we add 60 here due to the lag caused while running the test
                                true)
                ),
                Arguments.of(
                        new TestDataSet(
                                Instant.now().getEpochSecond() - Duration.ofDays(7).toSeconds() - 60, // testing edge cases, we minus 60 here due to the lag caused while running the test
                                false)
                ),
                Arguments.of(
                        new TestDataSet(
                                Instant.now().getEpochSecond(),
                                true)
                ),
                Arguments.of(
                        new TestDataSet(
                                Instant.now().getEpochSecond() + Duration.ofMinutes(30).toSeconds(),
                                true)
                ),
                Arguments.of(
                        new TestDataSet(
                                Instant.now().getEpochSecond() + Duration.ofMinutes(31).toSeconds(),
                                false)
                )

        );
    }

    @ParameterizedTest
    @MethodSource("dataSetProvider")
    @DisplayName("Filters out records if a record is older than 60 days. Will return true for records that have a timestamp that is at most 30 minutes in the future." +
            "Should return True for values that should be retained, false for values to be filtered out.")
    public void testFilterFunction(TestDataSet testDataSet) {

        RecordSetProto.RecordSet recordset = buildSimpleRecordSetProto(testDataSet.getRecordTimestamp());

        RecordTimeRangeFilter function = new RecordTimeRangeFilter(false);

        Assertions.assertEquals(function.filter(new Tuple2<>(recordset.getSensorId(), recordset.getRecord(0))), testDataSet.shouldBeRetained);
    }

    private RecordSetProto.RecordSet buildSimpleRecordSetProto(long timestamp) {
        String deviceId = UUID.randomUUID().toString();
        RecordSetProto.RecordSet.Record.ChannelSample channelSample = RecordSetProto.RecordSet.Record.ChannelSample.newBuilder().setDeviceId(deviceId).build();
        RecordSetProto.RecordSet.Record record = RecordSetProto.RecordSet.Record.newBuilder().setTimestamp(timestamp).addChannel(channelSample).build();
        return RecordSetProto.RecordSet.newBuilder().addRecord(record).build();
    }

    @Data
    @AllArgsConstructor
    private static class TestDataSet {
        private long recordTimestamp;
        private boolean shouldBeRetained;
    }
}
