package com.neurio.app.api.services.essdp;

import com.generac.ces.essdataprovider.model.PagedResponseDto;
import com.neurio.app.dataframe.component.Raw;
import com.neurio.app.dto.SystemEnergyDto;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class EssDpServiceStub implements IEssDpService {

    @Override
    public CompletableFuture<PagedResponseDto<SystemEnergyDto>> getEssEnergyByStartAndEndTimes(String systemId, Long prevTimestamp, Long currTimestamp) {

        PagedResponseDto pagedDto = new PagedResponseDto();
        SystemEnergyDto dto = new SystemEnergyDto();
        SystemEnergyDto.Raw raw = new SystemEnergyDto.Raw();
        SystemEnergyDto.Raw.RawData inverter = new SystemEnergyDto.Raw.RawData();

        // swapped
        if(systemId.equals("systemId-000100073534")) {

            double rawImpCurr = 40481738496.0d;
            double rawImpPrev = 40481590560.0d;
            double rawExpCurr = 1318551604.0d;
            double rawExpPrev = 1318551904.0d;
            inverter.setLifeTimeImported_Ws(rawImpCurr);
            inverter.setLifeTimeExported_Ws(rawExpCurr);
            inverter.setCurrentImport_Ws(rawImpCurr - rawImpPrev);
            inverter.setCurrentExport_Ws(rawExpCurr - rawExpPrev);
        }
        // not swapped
        else if(systemId.equals("systemId-000100093534")){

            double rawImpCurr = 1318551604.0d;
            double rawImpPrev = 1318551904.0d;
            double rawExpCurr = 40481738496.0d;
            double rawExpPrev = 40481590560.0d;
            inverter.setLifeTimeImported_Ws(rawImpCurr);
            inverter.setLifeTimeExported_Ws(rawExpCurr);
            inverter.setCurrentImport_Ws(rawImpCurr - rawImpPrev);
            inverter.setCurrentExport_Ws(rawExpCurr - rawExpPrev);
        }
        // same imported/exported deltas (not swapped)
        else {
            inverter.setCurrentImport_Ws(12345.0d);
            inverter.setCurrentExport_Ws(12345.0d);
        }

        raw.setInverter(inverter);
        dto.setRaw(raw);
        List<SystemEnergyDto> data = new ArrayList<>();
        data.add(dto);
        pagedDto.setData(data);
        return CompletableFuture.supplyAsync(() -> pagedDto);
    }
}