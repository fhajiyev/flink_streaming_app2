package com.neurio.app.common;

import com.neurio.app.config.AppConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.params.SetParams;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;

@Slf4j
public class RedisCache<V extends Serializable> implements Cache<String, V>, AutoCloseable {


    private JedisPool pool;

    public RedisCache(AppConfig.CacheConfig cacheConfig) {
        GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
        poolConfig.setMinIdle(0);
        pool = new JedisPool(poolConfig, cacheConfig.getHostUrl(), cacheConfig.getPort(), 0);

    }

    @Override
    public @Nullable
    V get(String key) {

        try (Jedis client = pool.getResource()) {
            byte[] val = client.get(key.getBytes(StandardCharsets.UTF_8));
            return val == null ? null : SerializationUtils.deserialize(val);
        } catch (Exception e) {
            log.warn("Failed to execute get() command with exception {}", key, ExceptionUtils.getRootCause(e));
            return null;
        }
    }

    @Override
    public void set(String key, V val) {
        try (Jedis client = pool.getResource()) {
            log.debug("saving key {} with val {}", key, val);
            String res = client.set(key.getBytes(StandardCharsets.UTF_8), SerializationUtils.serialize(val));
            if (res == null) {
                log.warn("failed to save to redis");
            } else {
                log.debug("saved key:{} to redis", key);
            }
        } catch (Exception e) {
            log.warn("Failed to execute set({}, {}) redis command with exception {}", key, val, ExceptionUtils.getRootCause(e));
        }
    }

    @Override
    public void set(String key, V val, long ttl) {

        log.debug("saving key {} with val {}", key, val);
        try (Jedis client = pool.getResource()) {
            String res = client.set(key.getBytes(StandardCharsets.UTF_8), SerializationUtils.serialize(val), SetParams.setParams().ex(ttl));
            if (res == null) {
                log.warn("failed to save to redis");
            } else {
                log.debug("saved key:{} to redis", key);
            }
        } catch (Exception e) {
            log.warn("Failed to execute set({}, {}, {}) redis command with exception {}", key, val, ttl, ExceptionUtils.getMessage(e));
        }
    }

    @Override
    public void clear(String key) {
        try (Jedis client = pool.getResource()) {
            Long res = client.del(key);
            if (res == null) {
                log.warn("failed to delete {} from redis", key);
            } else {
                log.debug("deleted key:{} with result {} from redis", key, res);
            }
        } catch (Exception e) {
            log.warn("Failed to execute redis command with exception {}", ExceptionUtils.getRootCauseMessage(e));
        }
    }

    @Override
    public void clearAll() {
        try (Jedis client = pool.getResource()) {
            String res = client.flushAll();
            if (res == null) {
                log.warn("failed to flush all from redis");
            } else {
                log.debug("deleted all keys {}", res);
            }
        } catch (Exception e) {
            log.warn("Failed to execute redis command with exception {}", ExceptionUtils.getMessage(e));
        }
    }

    @Override
    public void close() {
        pool.close();
    }
}
