package com.neurio.app.api.services.metrics;

import com.google.gson.annotations.SerializedName;
import lombok.Data;

import java.util.Map;

@Data
public class CountMetric implements BaseMetric {
    @SerializedName(value = "interval.ms")
    private long intervalMs;
    private long value;
    private String name;
    private String type;
    private long timestamp;
    private Map<String,String> attributes;
}
