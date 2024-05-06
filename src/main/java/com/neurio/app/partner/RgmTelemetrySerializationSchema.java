package com.neurio.app.partner;

import com.generac.ces.GeneracEventProtos;
import com.neurio.app.dataframe.DataFrame;
import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.connectors.flink.serialization.KinesisSerializationSchema;

import java.nio.ByteBuffer;

@Slf4j
public class RgmTelemetrySerializationSchema implements KinesisSerializationSchema<DataFrame> {
    @Override
    public ByteBuffer serialize(DataFrame dataFrame) {
        GeneracEventProtos.GeneracEvent.Builder generacEventBuilder = GeneracEventProtos.GeneracEvent.newBuilder();
        generacEventBuilder.setSystemId(dataFrame.getSystemMetaData().getSystemId());
        generacEventBuilder.setSiteId(dataFrame.getSystemMetaData().getSiteId());
        generacEventBuilder.setEnergyRecordSet(new RgmEnergyTelemetry(dataFrame).asProto());
        return ByteBuffer.wrap(generacEventBuilder.build().toByteArray());
    }

    @Override
    public String getTargetStream(DataFrame dataFrame) {
        return null;
    }

}
