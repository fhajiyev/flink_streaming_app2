package com.neurio.app.common;

import java.io.Serializable;

public interface Cache<String, V extends Serializable> {

    V get(String key);
    void set(String key, V v);
    void set(String k, V v, long ttl);
    void clear(String k);
    void clearAll();
}
