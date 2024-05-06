package com.neurio.app.source.kinesis.deserializer;

import com.neurio.app.protobuf.RecordSetProto;
import com.neurio.app.source.kinesis.deserializer.RecordSetDeserializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;

@Tag("UnitTest")
public class RecordSetDeserializerTest {

    private RecordSetDeserializer recordSetDeserializer = new RecordSetDeserializer();


    @Test
    public void testDeserialize() throws IOException {
        RecordSetProto.RecordSet expected = getRandomProto();
        RecordSetProto.RecordSet actual = recordSetDeserializer.deserialize(expected.toByteArray());
        Assertions.assertEquals(expected, actual);
    }

    @Test void testDeserialize_randomBytes() {
        byte[] randomBytes = "  hello world.".getBytes();
        Assertions.assertThrows(IOException.class, () -> recordSetDeserializer.deserialize(randomBytes));
    }

    private RecordSetProto.RecordSet getRandomProto() {
        RecordSetProto.RecordSet.Record.ChannelSample channelSample = RecordSetProto.RecordSet.Record.ChannelSample.newBuilder().setDeviceId("device-id1").build();
        RecordSetProto.RecordSet.Record record = RecordSetProto.RecordSet.Record.newBuilder().setTimestamp(300).addChannel(channelSample).build();
        RecordSetProto.RecordSet recordSet = RecordSetProto.RecordSet.newBuilder().addRecord(record).build();
        return recordSet;
    }
}
