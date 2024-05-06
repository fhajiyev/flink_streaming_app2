package com.neurio.app;

import com.neurio.app.config.AppConfig;
import com.neurio.app.config.utils.ParameterToolUtils;
import com.neurio.app.job.FlinkJob;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.util.ConfigurationException;

import java.io.IOException;
import java.util.Properties;

@Slf4j
public class App {

    public static void main(String[] args) throws ConfigurationException, IOException {

        ParameterToolUtils parameterToolUtils = new ParameterToolUtils();
        AppConfig appConfig = new AppConfig(parameterToolUtils);
        log.info("appconfig: {}", appConfig.toString());
        log.info("starting..");
        FlinkJob flinkJob = new FlinkJob(appConfig);
        flinkJob.run();
    }
}
