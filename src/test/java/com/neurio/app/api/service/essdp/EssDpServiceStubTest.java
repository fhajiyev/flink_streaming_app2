package com.neurio.app.api.service.essdp;

import com.neurio.app.api.services.essdp.EssDpServiceStub;
import com.neurio.app.api.services.essdp.IEssDpService;
import com.neurio.app.api.services.system.ISystemService;
import com.neurio.app.api.services.system.SystemServiceStub;
import com.neurio.app.dataframe.DataFrame;
import com.neurio.app.dataframe.component.SystemMetaData;
import com.neurio.app.dto.SystemEnergyDto;
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
public class EssDpServiceStubTest {

    @ParameterizedTest()
    @CsvSource({"000100073534", "000100093534"})
    public void testGetEssEnergyByStartAndEndTimes(String hostrcpn) throws ExecutionException, InterruptedException {
        IEssDpService essDpService = new EssDpServiceStub();

        SystemEnergyDto systemEnergyDto = essDpService.getEssEnergyByStartAndEndTimes("systemId-" + hostrcpn, 0L, 0L).get().getData().get(0);

        if(hostrcpn.equals("000100073534")) {
            Assertions.assertEquals(systemEnergyDto.getRaw().getInverter().getCurrentImport_Ws(), 40481738496.0d - 40481590560.0d);
            Assertions.assertEquals(systemEnergyDto.getRaw().getInverter().getCurrentExport_Ws(), 1318551604.0d - 1318551904.0d);
        } else {
            Assertions.assertEquals(systemEnergyDto.getRaw().getInverter().getCurrentImport_Ws(), 1318551604.0d - 1318551904.0d);
            Assertions.assertEquals(systemEnergyDto.getRaw().getInverter().getCurrentExport_Ws(), 40481738496.0d - 40481590560.0d);
        }
    }
}
