package com.neurio.app.api.services.system;

import java.util.concurrent.CompletableFuture;

public interface ISystemService {

    CompletableFuture<SystemResponseDto> getSystemByHostRcpn(String hostRcpn);
}
