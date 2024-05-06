package com.neurio.app.dataframe;

import com.generac.ces.EnergyDataframeProto;
import com.generac.ces.GeneracEventProtos;
import com.neurio.app.partner.RgmEnergyTelemetry;
import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.connectors.flink.serialization.KinesisSerializationSchema;

import java.nio.ByteBuffer;

@Slf4j
public class DataframeSerializationSchema implements KinesisSerializationSchema<DataFrame> {
    @Override
    public ByteBuffer serialize(DataFrame dataFrame) {

        EnergyDataframeProto.EnergyDataframe.Builder energyDataframeBuilder = EnergyDataframeProto.EnergyDataframe.newBuilder();
        energyDataframeBuilder.setTimestampUtc(dataFrame.getTimestamp_utc());
        energyDataframeBuilder.setTimezone(dataFrame.getSystemMetaData().isRegisteredSystem() ? dataFrame.getSystemMetaData().getTimezone() : "UTC");
        energyDataframeBuilder.setHostRcpn(dataFrame.getSystemMetaData().getHostRcpn());
        energyDataframeBuilder.setSystemId(dataFrame.getSystemMetaData().getSystemId());
        energyDataframeBuilder.setDataframeType(EnergyDataframeProto.DataframeType.RGM);
        energyDataframeBuilder.setSystemRegistrationType(dataFrame.getSystemMetaData().isRegisteredSystem() ? EnergyDataframeProto.SystemRegistrationType.REGISTERED : EnergyDataframeProto.SystemRegistrationType.UNREGISTERED);

        EnergyDataframeProto.RawData.Builder rawDataBuilder = EnergyDataframeProto.RawData.newBuilder();
        rawDataBuilder.setRawTimestamp(dataFrame.getEnergyMetaData().getRaw().getEvent_timestamp());
        if (dataFrame.getEnergyMetaData().getRaw().getRaw_inverter_lifetime_exported_Ws() != null) {
            rawDataBuilder.setRawInverterLifetimeExportedWs(dataFrame.getEnergyMetaData().getRaw().getRaw_inverter_lifetime_exported_Ws());
        }
        if (dataFrame.getEnergyMetaData().getRaw().getRaw_inverter_lifetime_imported_Ws() != null) {
            rawDataBuilder.setRawInverterLifetimeImportedWs(dataFrame.getEnergyMetaData().getRaw().getRaw_inverter_lifetime_imported_Ws());
        }
        energyDataframeBuilder.setRawData(rawDataBuilder.build());

        EnergyDataframeProto.DeltaData.Builder deltaDataBuilder = EnergyDataframeProto.DeltaData.newBuilder();
        deltaDataBuilder.setTimeDiffInSeconds(dataFrame.getEnergyMetaData().getEnergyDelta().getTime_delta_seconds());
        deltaDataBuilder.setInverterEnergyExportedWs(dataFrame.getEnergyMetaData().getEnergyDelta().getInverter_energy_exported_delta_Ws());
        deltaDataBuilder.setInverterEnergyImportedWs(dataFrame.getEnergyMetaData().getEnergyDelta().getInverter_energy_imported_delta_Ws());
        energyDataframeBuilder.setDeltaData(deltaDataBuilder.build());

        EnergyDataframeProto.RgmData.Builder rgmDataBuilder = EnergyDataframeProto.RgmData.newBuilder();
        rgmDataBuilder.setRealPowerW(dataFrame.getEnergyMetaData().getRaw().getRealPower_W());
        rgmDataBuilder.setVoltageV(dataFrame.getEnergyMetaData().getRaw().getVoltage_V());
        rgmDataBuilder.setReactivePowerVAR(dataFrame.getEnergyMetaData().getRaw().getReactivePower_VAR());
        rgmDataBuilder.setIsInverterOutputSwapped(dataFrame.getEnergyMetaData().getIsInverterOutputSwapped());
        energyDataframeBuilder.setRgmData(rgmDataBuilder.build());

        return ByteBuffer.wrap(energyDataframeBuilder.build().toByteArray());

    }

    @Override
    public String getTargetStream(DataFrame dataFrame) {
        return null;
    }

}

