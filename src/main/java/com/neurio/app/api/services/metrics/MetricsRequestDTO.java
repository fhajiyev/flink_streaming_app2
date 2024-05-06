package com.neurio.app.api.services.metrics;

import lombok.Data;

import java.util.List;

@Data
public class MetricsRequestDTO {
    List<BaseMetric> metrics;
}

