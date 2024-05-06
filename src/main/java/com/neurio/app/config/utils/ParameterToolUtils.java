package com.neurio.app.config.utils;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.StringUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Slf4j
@NoArgsConstructor
public class ParameterToolUtils {

    private String configFilePath;

    public ParameterToolUtils(String configResourceFile) {
        this.configFilePath = getClass()
                .getClassLoader()
                .getResource(configResourceFile)
                .getPath();
    }

    public ParameterTool fromApplicationProperties(String groupId) throws IOException, ConfigurationException {
        Properties flinkProperties;
        if (!StringUtils.isNullOrWhitespaceOnly(configFilePath)) {   // local testing
            flinkProperties = KinesisAnalyticsRuntime.getApplicationProperties(configFilePath).get(groupId);
        } else {
            flinkProperties = KinesisAnalyticsRuntime.getApplicationProperties().get(groupId);
        }
        if (flinkProperties == null) {
            log.error("Configuration for groupId {} not found", groupId);
            throw new ConfigurationException("Configuration not found");
        }
        return fromJavaProperties(flinkProperties);
    }

    public ParameterTool fromJavaProperties(Properties properties) {
        Map<String, String> map = new HashMap<>(properties.size());
        properties.forEach((k, v) -> map.put((String) k, (String) v));
        return ParameterTool.fromMap(map);
    }

}