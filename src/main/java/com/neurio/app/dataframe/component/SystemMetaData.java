package com.neurio.app.dataframe.component;

import lombok.*;

import java.io.Serializable;

@Builder
@Data
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class SystemMetaData implements Serializable {
    private static long serialVersionUID = 2405172041950251807L;
    private boolean registeredSystem;
    private String systemId;
    private String hostRcpn;
    private String timezone;
    private String locationId;
    private String siteId;
}