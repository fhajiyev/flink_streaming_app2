package com.neurio.app.testingutils.testcontainer.redis;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

public class RedisContainer extends GenericContainer<RedisContainer>{


    private RedisContainer() {
        super(DockerImageName.parse("redis:6.2.5-alpine"));
    }

    public static RedisContainer getInstance() {
        return LazyHolder.INSTANCE;
    }

    private static class LazyHolder {
        static final RedisContainer INSTANCE = new RedisContainer()
                .withExposedPorts(6379)
                //.withReuse(true)
                .waitingFor(Wait.forLogMessage(".*Ready to accept connections.*\\n", 1));
    }

    @Override
    public void start() {
        super.start();
    }

    @Override
    public void stop() {
        //do nothing, JVM handles shut down
    }
}
