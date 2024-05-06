package com.neurio.app.partner;


import com.neurio.app.dataframe.DataFrame;
import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.connectors.flink.KinesisPartitioner;

@Slf4j
public class RgmTelemetryKinesisPartitioner extends KinesisPartitioner<DataFrame> {
    @Override
    public String getPartitionId(DataFrame dataFrame) {
            return dataFrame.getSystemMetaData().getHostRcpn();
    }
}
