package com.neurio.app.common.utils;

import lombok.AllArgsConstructor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@Tag("UnitTest")
public class TimeUtilsTest {

    static Stream<Arguments> roundDownToNearestTimestampDataSet() {
        return Stream.of(
                Arguments.of(
                        new TestDataSet(
                                300,
                                TimeUnit.MINUTES,
                                5,
                                300),
                        new TestDataSet(
                                300,
                                TimeUnit.HOURS,
                                1,
                                0),
                        new TestDataSet(
                                599,
                                TimeUnit.MINUTES,
                                5,
                                300),
                        new TestDataSet(
                                690,
                                TimeUnit.MINUTES,
                                5,
                                600)
                ));
    }

    @ParameterizedTest
    @MethodSource("roundDownToNearestTimestampDataSet")
    public void testRoundDownToNearest(TestDataSet testDataSet) {
        long result = TimeUtils.roundDownToNearestTimestamp(testDataSet.timestamp, testDataSet.interval, testDataSet.timeUnit);
        Assertions.assertEquals(testDataSet.expectedValue, result);
    }

    @AllArgsConstructor
    private static class TestDataSet {
        private long timestamp;
        private TimeUnit timeUnit;
        private int interval;
        private long expectedValue;

    }
}
