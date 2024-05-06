package com.neurio.app.protobuf.utils;

import com.neurio.app.protobuf.RecordSetProto;

import java.util.HashMap;
import java.util.Map;

public class RecordMapper {


    public static Map<RecordSetProto.RecordSet.Record.ChannelSample.ChannelType, RecordSetProto.RecordSet.Record.ChannelSample> toRecordMap(RecordSetProto.RecordSet.Record record) {

        HashMap hashMap = new HashMap<>();

        for (RecordSetProto.RecordSet.Record.ChannelSample channelSample : record.getChannelList()) {
            if (channelSample.hasChannelType()) {
                hashMap.put(channelSample.getChannelType(), channelSample);
            }
        }

        return hashMap;
    }
}
