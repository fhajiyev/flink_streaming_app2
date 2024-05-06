package com.neurio.app.utils;

import com.generac.ces.essdataprovider.model.PagedResponseDto;
import com.google.gson.Gson;
import com.neurio.app.dto.SystemEnergyDto;

import java.lang.reflect.Type;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class Helper {
    private static Gson gson = new Gson();

    public static String toJsonString(Object obj) {
        return gson.toJson(obj);
    }

    public static PagedResponseDto<SystemEnergyDto> deserializeJson(String json, Type type) {
        return gson.fromJson(json, type);
    }

    public static String createFilePath(String hostRcpn, long timestamp) {
        ZonedDateTime sensorLocalDateTime = Instant.ofEpochSecond(timestamp).atZone(ZoneId.of("UTC"));
        String yyyyMMdd = LocalDate.of(sensorLocalDateTime.getYear(), sensorLocalDateTime.getMonth(), sensorLocalDateTime.getDayOfMonth()).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        return String.format("%s/%s/%s.json", yyyyMMdd, hostRcpn, timestamp);
    }
}
