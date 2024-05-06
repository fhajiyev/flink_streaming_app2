package com.neurio.app.config;

import com.neurio.app.config.utils.ParameterToolUtils;
import com.neurio.app.utils.NewRelicSecret;
import com.neurio.app.utils.SecretsManagerUtils;
import lombok.*;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.ConfigurationException;

import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;

@Getter
@Setter
@ToString
public class AppConfig implements Serializable {
    private static final long serialVersionUID = 2405172041950251807L;

    private GeneralConfig generalConfig;
    private KinesisSourceConfig kinesisSourceConfig;
    private DataframeOutputStreamConfig dataframeOutputStreamConfig;
    private PartnerRgmOutputStreamConfig partnerRgmOutputStreamConfig;
    private SystemServiceApiConfig systemServiceApiConfig;
    private CacheConfig cacheConfig;
    private DlqConfig dlqConfig;
    private NewRelicConfig newRelicConfig;
    private EssDpServiceApiConfig essDpServiceApiConfig;

    private boolean isLocalDevelopment = false;

    public AppConfig(ParameterToolUtils parameterToolUtils) throws ConfigurationException, IOException {

        ParameterTool param;
        param = parameterToolUtils.fromApplicationProperties("general");
        this.generalConfig = GeneralConfig.builder()
                .deploymentType(param.get("DeploymentType"))
                .build();

        param = parameterToolUtils.fromApplicationProperties("kinesisSource");
        this.kinesisSourceConfig = KinesisSourceConfig.builder()
                .consumerRecordPublisherType(param.get("RecordPublisherType"))
                .consumerEfoConsumerName(param.get("EfoConsumerName"))
                .initialStreamPosition(param.get("InitialStreamPosition"))
                .initialStreamTimestamp(param.get("InitialStreamTimestamp"))
                .kinesisEndpoint(param.get("Endpoint"))
                .awsRegion(param.get("Region"))
                .streamName(param.get("StreamName"))
                .build();

        param = parameterToolUtils.fromApplicationProperties("dataframeKinesisSink");
        this.dataframeOutputStreamConfig = DataframeOutputStreamConfig.builder()
                .isKinesisSinkEnabled(param.getBoolean("Enable"))
                .awsRegion(param.get("Region"))
                .streamName(param.get("StreamName"))
                .kinesisEndpoint(param.get("Endpoint"))
                .build();

        param = parameterToolUtils.fromApplicationProperties("partnerRgmKinesisSink");
        this.partnerRgmOutputStreamConfig = PartnerRgmOutputStreamConfig.builder()
                .isKinesisSinkEnabled(param.getBoolean("Enable"))
                .awsRegion(param.get("Region"))
                .streamName(param.get("StreamName"))
                .kinesisEndpoint(param.get("Endpoint"))
                .build();

        param = parameterToolUtils.fromApplicationProperties("redisCache");
        this.cacheConfig = CacheConfig.builder()
                .hostUrl(param.get("HostUrl"))
                .port(Integer.parseInt(param.get("Port")))
                .cacheDuration(Duration.ofHours(4).toSeconds())
                .jitterEnabled(true)
                .jitterLowerBoundSecond(0L)
                .jitterUpperBoundSecond(300L)
                .build();

        param = parameterToolUtils.fromApplicationProperties("systemService");
        this.systemServiceApiConfig = SystemServiceApiConfig.builder()
                .useStub(param.getBoolean("UseStub"))
                .systemHostUrl(param.get("HostUrl"))
                .systemEndpoint(param.get("InternalEndpoint"))
                .cacheConfig(cacheConfig)
                .build();

        param = parameterToolUtils.fromApplicationProperties("dlq");
        this.dlqConfig = DlqConfig.builder()
                .bucket(param.get("Bucket"))
                .build();

        param = parameterToolUtils.fromApplicationProperties("newRelic");
        NewRelicSecret newRelicSecret = SecretsManagerUtils.getNewRelicSecret(param.get("Secret"));
        this.newRelicConfig = NewRelicConfig.builder()
                .endpoint(param.get("Endpoint"))
                .ingestApiKey(newRelicSecret.getApiKey())
                .build();

        param = parameterToolUtils.fromApplicationProperties("essDpService");
        this.essDpServiceApiConfig = EssDpServiceApiConfig.builder()
                .useStub(param.getBoolean("UseStub", false))
                .essDpHostUrl(param.get("HostUrl", "http://pwrview-dev.neurio.internal"))
                .essDpEndpoint(param.get("InternalEndpoint", "/ess-dataprovider/v1/systems"))
                .timeout(param.getLong("RequestTimeoutInSeconds", Duration.ofSeconds(5).toSeconds()))
                .build();

    }

    public AppConfig() {

    }

    @Getter
    @Builder
    @ToString
    public static class GeneralConfig implements Serializable {
        private static final long serialVersionUID = 2405172041950251807L;
        private final String deploymentType;
    }

    @Getter
    @Builder
    @ToString
    public static class KinesisSourceConfig implements Serializable {
        private static final long serialVersionUID = 2405172041950251807L;
        private final String consumerRecordPublisherType;
        private final String consumerEfoConsumerName;
        private final String initialStreamPosition;
        private final String initialStreamTimestamp;
        private final String kinesisEndpoint;
        private final String awsRegion;
        private final String streamName;
    }

    @Getter
    @Builder
    @ToString
    public static class SystemServiceApiConfig implements Serializable {
        private static final long serialVersionUID = 2405172041950251807L;
        private final boolean useStub; // for local testing
        private final String systemHostUrl;
        private final String systemEndpoint;
        private final CacheConfig cacheConfig;
    }

    @Getter
    @Builder
    @ToString
    public static class DataframeOutputStreamConfig implements Serializable {
        private static final long serialVersionUID = 2405113141950251807L;
        private boolean isKinesisSinkEnabled;
        private final String awsRegion;
        private final String streamName;
        private final String kinesisEndpoint;
    }

    @Getter
    @Builder
    @ToString
    public static class PartnerRgmOutputStreamConfig implements Serializable {
        private static final long serialVersionUID = 2405113141950251807L;
        private boolean isKinesisSinkEnabled;
        private final String awsRegion;
        private final String streamName;
        private final String kinesisEndpoint;
    }

    @Getter
    @Builder
    @ToString
    public static class CacheConfig implements Serializable {
        private static final long serialVersionUID = 2222L;
        private final String hostUrl;
        private final int port;
        private long cacheDuration;
        private boolean jitterEnabled;
        private long jitterLowerBoundSecond;
        private long jitterUpperBoundSecond;
    }

    @Getter
    @Builder
    @ToString
    public static class DlqConfig implements Serializable {
        private static final long serialVersionUID = 2405172041950251807L;
        private final String bucket;
    }

    @Getter
    @Builder
    @ToString
    public static class NewRelicConfig implements Serializable {
        private static final long serialVersionUID = 2405172041950251807L;
        private final String endpoint;
        private final String ingestApiKey;
    }

    @Getter
    @Builder
    @ToString
    public static class EssDpServiceApiConfig implements Serializable {
        private static final long serialVersionUID = 2405172041950251807L;
        private final boolean useStub; // for local testing
        private final String essDpHostUrl;
        private final String essDpEndpoint;
        private final long timeout;
    }
}
