package com.neurio.app.filter;

import com.neurio.app.dataframe.DataFrame;
import com.neurio.app.protobuf.RecordSetProto.*;
import org.apache.flink.api.common.functions.FilterFunction;

import java.util.List;

public class RgmRecordValidationFilter implements FilterFunction<DataFrame> {

    private final boolean isLocalDevelopment;

    public RgmRecordValidationFilter(boolean isLocalDevelopment) {
        this.isLocalDevelopment = isLocalDevelopment;
    }

    @Override
    public boolean filter(DataFrame df) {

        if (isLocalDevelopment)
            return true;

        if(df.getEnergyMetaData()==null || df.getEnergyMetaData().getCurrentRecord()==null)
            return false;

        RecordSet.Record record = df.getEnergyMetaData().getCurrentRecord();
        List<RecordSet.Record.ChannelSample> list = record.getChannelList();
        for (RecordSet.Record.ChannelSample channelSample : list) {
            if (channelSample.hasChannelType()
                    && channelSample.getChannelType().equals(RecordSet.Record.ChannelSample.ChannelType.INVERTER_OUTPUT)
                    && channelSample.hasDeviceType()
                    && channelSample.getDeviceType().equals(RecordSet.Record.ChannelSample.DeviceType.INVERTER)
                    && channelSample.hasDeviceId()) {
                return true;
            }
        }

        return false;
    }
}
