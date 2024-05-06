package com.neurio.app.api.services.essdp;

import com.generac.ces.essdataprovider.model.PagedResponseDto;
import com.neurio.app.dto.SystemEnergyDto;

import java.util.concurrent.CompletableFuture;

public interface IEssDpService {

    CompletableFuture<PagedResponseDto<SystemEnergyDto>> getEssEnergyByStartAndEndTimes(String systemId, Long prevTimestamp, Long currTimestamp);
}