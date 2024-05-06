package com.neurio.app.api.services.system;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class SystemServiceStub implements ISystemService {

    // hostrcpn, (timestamp, #of times called)
    ConcurrentHashMap<String, Tuple2<Long, Integer>> map = new ConcurrentHashMap<>();

    private List<String> timezones = Arrays.asList("America/New_York", "America/Chicago", "America/Los_Angeles", "Pacific/Honolulu");

    public CompletableFuture<SystemResponseDto> getSystemByHostRcpn(String hostRcpn) {
        if (map.containsKey(hostRcpn)) {
            int count = map.get(hostRcpn).f1;
            long lastFetchTime = map.get(hostRcpn).f0;
            map.put(hostRcpn, new Tuple2(Instant.now().toEpochMilli(), count + 1));
            log.info("fetching system info for {} {} times, last fetch happened {} ms ago", hostRcpn, count + 1, Instant.now().toEpochMilli() - lastFetchTime);
        } else {
            map.put(hostRcpn, new Tuple2(Instant.now().toEpochMilli(), 1));
            log.info("fetching system info for {} {} times", hostRcpn, 1);
        }
        return CompletableFuture.supplyAsync(() -> stubSystemResponse(hostRcpn));
    }


    private SystemResponseDto stubSystemResponse(String hostRcpn) {

        int seed = hostRcpn.hashCode();
        SystemResponseDto systemResponseDto = new SystemResponseDto();
        systemResponseDto.setHostRcpn(hostRcpn);
        systemResponseDto.setSystemId("systemId-" + hostRcpn);
        systemResponseDto.setSiteId("siteId-" + hostRcpn);
        systemResponseDto.setIsRegistered(seed % 10 > 0);

        if (systemResponseDto.getIsRegistered()) {
            int timezoneSeed = seed % timezones.size();

            systemResponseDto.setTimezone(timezones.get(timezoneSeed));
            systemResponseDto.setLocationId("locationId-" + hostRcpn);
        }
        return systemResponseDto;
    }
}
