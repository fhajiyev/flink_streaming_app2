package com.neurio.app.source.kinesis.deserializer;

import com.neurio.app.protobuf.RecordSetProto;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

import java.io.IOException;

@Slf4j
public class RecordSetDeserializer extends AbstractDeserializationSchema<RecordSetProto.RecordSet> {

    @Override
    public RecordSetProto.RecordSet deserialize(byte[] bytes) throws IOException {
        try {
            return RecordSetProto.RecordSet.parseFrom(bytes);
        } catch (IOException e) {
            log.error("oops");
            throw e;
        }
    }
}
