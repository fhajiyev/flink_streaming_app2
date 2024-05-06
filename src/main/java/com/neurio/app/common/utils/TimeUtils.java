package com.neurio.app.common.utils;

import java.util.concurrent.TimeUnit;

public class TimeUtils {

    /**
     *
     * will round 5m:30s to 5 minute, 4:30s to 5 minute
     *
     * @param timestamp an epoch second timestamp
     *
     * @return returns a timestamp nearest to the interval and Time unit ie (5 MINUTE,1 HOUR...)
     * */
    public static long getNearestTimeStamp(Number timestamp, int interval, TimeUnit timeUnit) {
        long frequency = getFrequencyInSeconds(interval, timeUnit);
        long half = frequency / 2;

        long r = timestamp.longValue() % frequency;

        if (r < half ) {
            return Math.round((timestamp.longValue() - r) / frequency) * frequency;
        } else {
            return Math.round((timestamp.longValue() + r) / frequency) * frequency;
        }

    }

    public static long roundDownToNearestTimestamp(Number timestamp,  int interval, TimeUnit timeUnit) {
        long r = timestamp.longValue() % getFrequencyInSeconds(interval, timeUnit);
        return  timestamp.longValue() - r;
    }

    private static long getFrequencyInSeconds(int frequency, TimeUnit timeUnit) {

        return TimeUnit.SECONDS.convert(frequency, timeUnit);
    }
}
