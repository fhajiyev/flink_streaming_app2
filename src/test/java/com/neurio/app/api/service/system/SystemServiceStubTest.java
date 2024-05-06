package com.neurio.app.api.service.system;

import com.neurio.app.api.services.system.ISystemService;
import com.neurio.app.api.services.system.SystemServiceStub;
import com.neurio.app.dataframe.DataFrame;
import com.neurio.app.dataframe.component.SystemMetaData;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.ZoneId;
import java.util.concurrent.ExecutionException;
@Slf4j
@Tag("UnitTest")
public class SystemServiceStubTest {

    @ParameterizedTest()
    @CsvSource({"000100073180", "0001000732F5"})
    public void testGetSystemByHostRcpn(String hostrcpn) throws ExecutionException, InterruptedException {
        ISystemService systemService = new SystemServiceStub();

        SystemMetaData systemMetaData = systemService.getSystemByHostRcpn(hostrcpn).get().toSystemMetaData();
        log.info("{} isRegistered = {}", hostrcpn, systemMetaData.isRegisteredSystem() );
        if (systemMetaData.isRegisteredSystem()) {

            Assertions.assertTrue(StringUtils.isNotBlank(systemMetaData.getTimezone()));
            Assertions.assertTrue(StringUtils.isNotBlank(systemMetaData.getLocationId()));
            Assertions.assertDoesNotThrow(() -> {
                ZoneId.of(systemMetaData.getTimezone());
            });
           ;
        }
        Assertions.assertTrue(StringUtils.isNotBlank(systemMetaData.getSystemId()));
        Assertions.assertTrue(StringUtils.isNotBlank(systemMetaData.getHostRcpn()));
        Assertions.assertTrue(StringUtils.isNotBlank(systemMetaData.getSiteId()));
    }
}
