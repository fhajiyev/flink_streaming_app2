package com.neurio.app.partner;

import com.generac.ces.system.SystemModeOuterClass;
import com.generac.ces.telemetry.ChannelSampleOuterClass;
import com.generac.ces.telemetry.EnergyDeviceTypeOuterClass;
import com.generac.ces.telemetry.EnergyRecordOuterClass;
import com.generac.ces.telemetry.EnergyRecordSetOuterClass;
import com.neurio.app.dataframe.DataFrame;
import com.neurio.app.protobuf.RecordSetProto.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.exception.ExceptionUtils;

@Slf4j
public class RgmEnergyTelemetry {

    private DataFrame dataTelemetry;

    public RgmEnergyTelemetry(DataFrame dataFrame) {
        this.dataTelemetry = dataFrame;
    }

    public EnergyRecordSetOuterClass.EnergyRecordSet asProto() {

        EnergyRecordSetOuterClass.EnergyRecordSet.Builder recordSetBuilder = EnergyRecordSetOuterClass.EnergyRecordSet.newBuilder();
        try {
            EnergyRecordOuterClass.EnergyRecord energyRecord = convertRecordSetProtoToEnergyRecordSet(dataTelemetry);
            recordSetBuilder.addEnergyRecord(energyRecord);
        } catch (Exception e) {
            log.warn("Could not convert recordset [{}] to partner RGM proto with exception {} ", dataTelemetry.getEnergyMetaData().getCurrentRecord(), ExceptionUtils.getStackTrace(e));
        }

        return recordSetBuilder.build();
    }

    private EnergyRecordOuterClass.EnergyRecord convertRecordSetProtoToEnergyRecordSet
            (DataFrame dataframe) {

        RecordSet.Record currentRecord = dataframe.getEnergyMetaData().getCurrentRecord();

        EnergyRecordOuterClass.EnergyRecord.Builder energyRecordBuilder = EnergyRecordOuterClass.EnergyRecord.newBuilder();

        energyRecordBuilder.setInverterState(dataTelemetry.getEnergyMetaData().getCurrentRecord().getInverterState().getValueDescriptor().getNumber());
        energyRecordBuilder.setSysMode(SystemModeOuterClass.SystemMode.valueOf(dataTelemetry.getEnergyMetaData().getCurrentRecord().getSysMode().getValueDescriptor().getName()));
        energyRecordBuilder.setCurrentTimestamp(dataTelemetry.getEnergyMetaData().getCurrentRecord().getTimestamp());
        energyRecordBuilder.setPreviousTimestamp(dataTelemetry.getEnergyMetaData().getCurrentRecord().getTimestamp() - dataTelemetry.getEnergyMetaData().getEnergyDelta().getTime_delta_seconds());

        currentRecord.getChannelList().forEach(channel -> {
            ChannelSampleOuterClass.ChannelSample.Builder channelSampleBuilder = ChannelSampleOuterClass.ChannelSample.newBuilder();

            switch (channel.getChannelType()) {
                case INVERTER_OUTPUT: {
                    channelSampleBuilder.setChannel(channel.getChannel());
                    channelSampleBuilder.setChannelType(ChannelSampleOuterClass.ChannelType.INVERTER_OUTPUT);
                    if (dataTelemetry.getEnergyMetaData().getEnergyDelta().getInverter_energy_exported_delta_Ws() != null) {
                        channelSampleBuilder.setDeltaExportedEnergyWs(dataTelemetry.getEnergyMetaData().getEnergyDelta().getInverter_energy_exported_delta_Ws());
                    }
                    if (dataTelemetry.getEnergyMetaData().getEnergyDelta().getInverter_energy_imported_delta_Ws() != null) {
                        channelSampleBuilder.setDeltaImportedEnergyWs(dataTelemetry.getEnergyMetaData().getEnergyDelta().getInverter_energy_imported_delta_Ws());
                    }

                    channelSampleBuilder.setExportedEnergyWs(dataTelemetry.getEnergyMetaData().getRaw().getRaw_inverter_lifetime_exported_Ws());
                    channelSampleBuilder.setImportedEnergyWs(dataTelemetry.getEnergyMetaData().getRaw().getRaw_inverter_lifetime_imported_Ws());

                    channelSampleBuilder.setDeviceType(EnergyDeviceTypeOuterClass.EnergyDeviceType.INVERTER);
                    channelSampleBuilder.setDeviceId(channel.getDeviceId());
                    channelSampleBuilder.setRealPowerW(channel.getRealPowerW());
                    channelSampleBuilder.setChannelTypeValue(ChannelSampleOuterClass.ChannelType.INVERTER_OUTPUT.getNumber());
                    channelSampleBuilder.setDeviceIdBytes(channel.getDeviceIdBytes());
                    channelSampleBuilder.setDeviceTypeValue(EnergyDeviceTypeOuterClass.EnergyDeviceType.INVERTER.getNumber());
                    channelSampleBuilder.setIsRevenueGrade(true);
                    energyRecordBuilder.addChannel(channelSampleBuilder);
                    break;
                }
                default:
                    log.info("Wrong channel type in received record: {}. Skipping | hostRcpn:{} | timestamp:{}",
                            channel.getChannelType(),
                            dataframe.getSystemMetaData().getHostRcpn(),
                            currentRecord.getTimestamp());
                    break;
            }
        });

        return energyRecordBuilder.build();
    }
}