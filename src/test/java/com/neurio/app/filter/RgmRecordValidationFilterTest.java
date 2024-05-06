package com.neurio.app.filter;

import com.neurio.app.dataframe.DataFrame;
import com.neurio.app.dataframe.component.EnergyMetaData;
import com.neurio.app.dataframe.component.SystemMetaData;
import com.neurio.app.protobuf.RecordSetProto.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.UUID;

@Tag("UnitTest")
public class RgmRecordValidationFilterTest {

    @Test
    public void testFilterFunction() {

        RgmRecordValidationFilter function = new RgmRecordValidationFilter(false);

        DataFrame df = new DataFrame();
        Assertions.assertEquals(function.filter(df), false);

        df.setEnergyMetaData(EnergyMetaData.builder().currentRecord(buildEmptyRecordProto()).build());
        Assertions.assertEquals(function.filter(df), false);

        df.setEnergyMetaData(EnergyMetaData.builder().currentRecord(buildIncorrectRecordProto()).build());
        Assertions.assertEquals(function.filter(df), false);

        df.setEnergyMetaData(EnergyMetaData.builder().currentRecord(buildCorrectRecordProtoNoDeviceId()).build());
        Assertions.assertEquals(function.filter(df), false);

        df.setEnergyMetaData(EnergyMetaData.builder().currentRecord(buildCorrectRecordProtoWithDeviceId()).build());
        Assertions.assertEquals(function.filter(df), true);

    }

    private RecordSet.Record buildCorrectRecordProtoNoDeviceId() {
        RecordSet.Record.ChannelSample channelSample = RecordSet.Record.ChannelSample.newBuilder()
                .setChannelType(RecordSet.Record.ChannelSample.ChannelType.INVERTER_OUTPUT)
                .setDeviceType(RecordSet.Record.ChannelSample.DeviceType.INVERTER)
                .build();
        return RecordSet.Record.newBuilder().addChannel(channelSample).build();
    }

    private RecordSet.Record buildCorrectRecordProtoWithDeviceId() {
        RecordSet.Record.ChannelSample channelSample = RecordSet.Record.ChannelSample.newBuilder()
                .setChannelType(RecordSet.Record.ChannelSample.ChannelType.INVERTER_OUTPUT)
                .setDeviceType(RecordSet.Record.ChannelSample.DeviceType.INVERTER)
                .setDeviceId("sample_host_rcpn")
                .build();
        return RecordSet.Record.newBuilder().addChannel(channelSample).build();
    }

    private RecordSet.Record buildIncorrectRecordProto() {
        RecordSet.Record.ChannelSample channelSample = RecordSet.Record.ChannelSample.newBuilder()
                .setChannelType(RecordSet.Record.ChannelSample.ChannelType.GENERATION)
                .setDeviceType(RecordSet.Record.ChannelSample.DeviceType.SOLAR)
                .setDeviceId("sample_host_rcpn")
                .build();
        return RecordSet.Record.newBuilder().addChannel(channelSample).build();
    }

    private RecordSet.Record buildEmptyRecordProto() {
        RecordSet.Record.ChannelSample channelSample = RecordSet.Record.ChannelSample.newBuilder()
                .build();
        return RecordSet.Record.newBuilder().addChannel(channelSample).build();
    }
}
