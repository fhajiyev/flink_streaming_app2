package com.neurio.app.common;

import org.apache.commons.lang3.tuple.Pair;

import java.io.Serializable;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;

public class RedisCacheStub<V extends Serializable> implements Cache<String, V> {

    private ConcurrentHashMap<String, Pair<V, Long>> map;

    public RedisCacheStub() {
        map = new ConcurrentHashMap<>();
    }


    @Override
    public V get(String key) {

        if (!map.containsKey(key)) {
            return null;
        }

        long now = Instant.now().getEpochSecond();

        // entry has no ttl
        if (map.get(key).getRight() == null) {
            return map.get(key).getLeft();
        }

        if (now > map.get(key).getRight()) {
            //expired
            return null;
        }

        return map.get(key).getLeft();
    }

    @Override
    public void set(String key, V v) {
        map.put(key, Pair.of(v, null));
    }

    @Override
    public void set(String k, V v, long ttl) {

        map.put(k, Pair.of(v, Instant.now().getEpochSecond() + ttl));
    }

    @Override
    public void clear(String k) {
        map.remove(k);
    }

    @Override
    public void clearAll() {
        map.clear();
    }
}
