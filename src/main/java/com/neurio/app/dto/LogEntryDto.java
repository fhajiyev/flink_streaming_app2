package com.neurio.app.dto;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;

@Builder
@ToString
@Getter
public class LogEntryDto implements Serializable {
    private String identifier;
    private long epochSeconds;
    private String eventType;
    private String msg;
}
