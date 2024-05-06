package com.neurio.app.testingutils;

import com.google.common.collect.ImmutableList;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.protobuf.util.JsonFormat;
import com.neurio.app.common.utils.TimeUtils;
import com.neurio.app.protobuf.RecordSetProto;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Assertions;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class RecordSetProtoProvider {

    public static ImmutableList<RecordSetProto.RecordSet> getRecordSetData(List<String> folders) throws IOException {
        ImmutableList.Builder<RecordSetProto.RecordSet> listBuilder = ImmutableList.<RecordSetProto.RecordSet>builder();
        List<File> files = new ArrayList<>();
        for (String folder : folders) {
            File file = new File(folder);
            files.addAll(Arrays.asList(file.listFiles()));
        }

        sortByFileNameTimestamps(files);

        for (File file : files) {
            RecordSetProto.RecordSet.Builder recordSetBuilder = RecordSetProto.RecordSet.newBuilder();
            JsonFormat.parser().merge(getJsonFromFile(file.getCanonicalPath()).toString(), recordSetBuilder);
            RecordSetProto.RecordSet recordSet = recordSetBuilder.build();
            // verify the file name and timestamp
            long recordTs = (long)recordSet.getRecord(0).getTimestamp();
            String[] p1 = file.getCanonicalPath().split(".json")[0].split(System.getProperty("os.name").toLowerCase().contains("window") ? "\\\\" : "/");
            long t1 = Long.parseLong(p1[p1.length-1]);
            long r1 = TimeUtils.getNearestTimeStamp(recordTs, 5, TimeUnit.MINUTES);
            long r2 = TimeUtils.getNearestTimeStamp(t1, 5, TimeUnit.MINUTES);
            if (r1 != r2) {
                throw new RuntimeException("d");
            }
            listBuilder.add(recordSet);
        }

        ImmutableList<RecordSetProto.RecordSet> dataSet =  listBuilder.build();

        // validate dataset is in order
        Long expectedTimestamp = null;
        for (RecordSetProto.RecordSet r : dataSet) {
            long ts = TimeUtils.getNearestTimeStamp((long) r.getRecord(0).getTimestamp(), 5, TimeUnit.MINUTES);
            if (expectedTimestamp == null) {
                expectedTimestamp = ts + 300;
            } else {
                Assertions.assertEquals(expectedTimestamp, ts);
                expectedTimestamp  += 300;
            }
        }

        return dataSet;
    }

    public static JsonObject getJsonFromFile(String filePath) throws IOException {
        InputStream is = new FileInputStream(filePath);
        String jsonTxt = IOUtils.toString(is);
        return JsonParser.parseString(jsonTxt).getAsJsonObject();
    }

    public static RecordSetProto.RecordSet getRecordSetFromFile(String filePath) throws IOException {
        RecordSetProto.RecordSet.Builder recordSetBuilder = RecordSetProto.RecordSet.newBuilder();
        JsonFormat.parser().merge(getJsonFromFile(filePath).toString(), recordSetBuilder);
        return recordSetBuilder.build();
    }

    private static void sortByFileNameTimestamps(List<File> list) {

        list.sort((a, b) -> {
            try {
                String[] p1 = a.getCanonicalPath().split(".json")[0].split(System.getProperty("os.name").toLowerCase().contains("window") ? "\\\\" : "/");
                String[] p2 = b.getCanonicalPath().split(".json")[0].split(System.getProperty("os.name").toLowerCase().contains("window") ? "\\\\" : "/");

                long t1 = Long.parseLong(p1[p1.length-1]);
                long t2 = Long.parseLong(p2[p2.length-1]);

                return (t1>t2) ? 1: -1;
            } catch (IOException e) {
                e.printStackTrace();
            }
            return 0;
        });
    }
}
