package com.neurio.app.api.services.system;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.neurio.app.dataframe.DataFrame;
import com.neurio.app.dataframe.component.SystemMetaData;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
@Slf4j
public class SystemResponseDto implements Serializable {
    private static final long serialVersionUID = 2405172041950251807L;
    private static String DEFAULT_TIMEZONE = "UNKNOWN";
    private String systemId;
    private String siteId;
    private String timezone;
    private Boolean isRegistered = false;
    private String hostRcpn;
    private String locationId;

    public SystemMetaData toSystemMetaData() {
        return SystemMetaData.builder()
                .hostRcpn(hostRcpn)
                .systemId(systemId)
                .siteId(siteId)
                .registeredSystem(isRegistered)
                .timezone(StringUtils.isNotBlank(timezone) ? timezone : DEFAULT_TIMEZONE)
                .locationId(locationId)
                .build();

    }
}
