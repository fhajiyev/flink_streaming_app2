package com.neurio.app.common;

import com.neurio.app.config.AppConfig;
import com.neurio.app.dataframe.DataFrame;
import com.neurio.app.dataframe.component.SystemMetaData;
import com.neurio.app.testingutils.testcontainer.redis.RedisContainer;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.org.apache.commons.lang.RandomStringUtils;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
@Tag("IntegrationTest")
public class RedisCacheTest {


    private static AppConfig.CacheConfig cacheConfig;

    @BeforeAll
    public static void setup() {

        RedisContainer redisContainer = RedisContainer.getInstance();
        redisContainer.start();

        cacheConfig = AppConfig.CacheConfig.builder()
                .hostUrl(redisContainer.getHost())
                .port(redisContainer.getFirstMappedPort())
                .build();
    }

    @Test
    public void testSet() {

        Cache<String, DataFrame> cache = new RedisCache<>(cacheConfig);
        String key = RandomStringUtils.randomAlphabetic(5);
        DataFrame input = new DataFrame();
        input.setSystemMetaData(SystemMetaData.builder()
                .hostRcpn("test-rcpn")
                .build());

        cache.set(key, input);

        DataFrame output = cache.get(key);

        Assertions.assertEquals(input, output);

    }


    @Test
    public void testSetMultiThreaded() throws InterruptedException {

        Cache<String, DataFrame> cache = new RedisCache<>(cacheConfig);
        String key1 = RandomStringUtils.randomAlphabetic(5);
        String key2 = RandomStringUtils.randomAlphabetic(5);
        DataFrame input = new DataFrame();
        input.setSystemMetaData(SystemMetaData.builder()
                .hostRcpn("test-rcpn")
                .build());

        Runnable r1 = () -> {
            cache.set(key1, input);
        };

        Runnable r2 = () -> {
            cache.set(key2, input);
        };

        ExecutorService jobExecutor = Executors.newCachedThreadPool();

        jobExecutor.submit(r1);
        jobExecutor.submit(r2);
        jobExecutor.awaitTermination(2, TimeUnit.SECONDS);
        DataFrame output1 = cache.get(key1);
        DataFrame output2 = cache.get(key2);

        Assertions.assertEquals(input, output1);
        Assertions.assertEquals(input, output2);
    }

    @Test
    public void testTtl() throws InterruptedException {
        Cache<String, DataFrame> cache = new RedisCache<>(cacheConfig);

        String key = RandomStringUtils.randomAlphabetic(5);
        DataFrame input = new DataFrame();
        input.setSystemMetaData(SystemMetaData.builder()
                .hostRcpn(key)
                .build());

        cache.set(key, input, Duration.ofSeconds(1).toSeconds());

        Thread.sleep(1500);
        DataFrame output = cache.get(key);

        Assertions.assertNull(output);
    }
}
