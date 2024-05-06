package com.neurio.app.protobuf.utils;

import com.neurio.app.protobuf.RecordSetProto;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

public class RecordSetExtractor {

    public static String getSensorId(RecordSetProto.RecordSet recordSet, String defaultVal) {
        return recordSet.hasSensorId() ? recordSet.getSensorId() : defaultVal;
    }

    public static class RecordExtractor {

        public static class ChannelSampleExtractor {

            // inverter imported/exported needs to be reversed for RGM
            public static Long getRaw_inverter_lifetime_imported_or_default_Ws(
                    Map<RecordSetProto.RecordSet.Record.ChannelSample.ChannelType, RecordSetProto.RecordSet.Record.ChannelSample> map, Long defaultVal) {
                if (map.containsKey(RecordSetProto.RecordSet.Record.ChannelSample.ChannelType.INVERTER_OUTPUT)) {
                    RecordSetProto.RecordSet.Record.ChannelSample channelSample = map.get(RecordSetProto.RecordSet.Record.ChannelSample.ChannelType.INVERTER_OUTPUT);

                    if (channelSample.hasDeviceType()
                            && channelSample.getDeviceType().equals(RecordSetProto.RecordSet.Record.ChannelSample.DeviceType.INVERTER)
                            && channelSample.hasExportedEnergyWs()) {
                        return channelSample.getExportedEnergyWs();
                    }
                }

                return defaultVal;

            }

            // inverter imported/exported needs to be reversed for RGM
            public static Long getRaw_inverter_lifetime_exported_Ws(
                    Map<RecordSetProto.RecordSet.Record.ChannelSample.ChannelType, RecordSetProto.RecordSet.Record.ChannelSample> map, Long defaultVal) {

                if (map.containsKey(RecordSetProto.RecordSet.Record.ChannelSample.ChannelType.INVERTER_OUTPUT)) {
                    RecordSetProto.RecordSet.Record.ChannelSample channelSample = map.get(RecordSetProto.RecordSet.Record.ChannelSample.ChannelType.INVERTER_OUTPUT);

                    if (channelSample.hasDeviceType()
                            && channelSample.getDeviceType().equals(RecordSetProto.RecordSet.Record.ChannelSample.DeviceType.INVERTER)
                            && channelSample.hasImportedEnergyWs()) {
                        return channelSample.getImportedEnergyWs();
                    }
                }

                return defaultVal;
            }

            public static Integer getRealPower_W(
                    Map<RecordSetProto.RecordSet.Record.ChannelSample.ChannelType, RecordSetProto.RecordSet.Record.ChannelSample> map, Integer defaultVal) {

                if (map.containsKey(RecordSetProto.RecordSet.Record.ChannelSample.ChannelType.INVERTER_OUTPUT)) {
                    RecordSetProto.RecordSet.Record.ChannelSample channelSample = map.get(RecordSetProto.RecordSet.Record.ChannelSample.ChannelType.INVERTER_OUTPUT);

                    if (channelSample.hasDeviceType()
                            && channelSample.getDeviceType().equals(RecordSetProto.RecordSet.Record.ChannelSample.DeviceType.INVERTER)
                            && channelSample.hasRealPowerW()) {
                        return Integer.valueOf(channelSample.getRealPowerW());
                    }
                }

                return defaultVal;
            }

            public static Float getVoltage_V(
                    Map<RecordSetProto.RecordSet.Record.ChannelSample.ChannelType, RecordSetProto.RecordSet.Record.ChannelSample> map, Float defaultVal) {

                if (map.containsKey(RecordSetProto.RecordSet.Record.ChannelSample.ChannelType.INVERTER_OUTPUT)) {
                    RecordSetProto.RecordSet.Record.ChannelSample channelSample = map.get(RecordSetProto.RecordSet.Record.ChannelSample.ChannelType.INVERTER_OUTPUT);

                    if (channelSample.hasDeviceType()
                            && channelSample.getDeviceType().equals(RecordSetProto.RecordSet.Record.ChannelSample.DeviceType.INVERTER)
                            && channelSample.hasVoltageV()) {
                        return Float.valueOf(channelSample.getVoltageV());
                    }
                }

                return defaultVal;
            }

            public static Integer getReactivePower_VAR(
                    Map<RecordSetProto.RecordSet.Record.ChannelSample.ChannelType, RecordSetProto.RecordSet.Record.ChannelSample> map, Integer defaultVal) {

                if (map.containsKey(RecordSetProto.RecordSet.Record.ChannelSample.ChannelType.INVERTER_OUTPUT)) {
                    RecordSetProto.RecordSet.Record.ChannelSample channelSample = map.get(RecordSetProto.RecordSet.Record.ChannelSample.ChannelType.INVERTER_OUTPUT);

                    if (channelSample.hasDeviceType()
                            && channelSample.getDeviceType().equals(RecordSetProto.RecordSet.Record.ChannelSample.DeviceType.INVERTER)
                            && channelSample.hasReactivePowerVAR()) {
                        return Integer.valueOf(channelSample.getReactivePowerVAR());
                    }
                }

                return defaultVal;
            }

            public static String getHost_Rcpn
                    (Map<RecordSetProto.RecordSet.Record.ChannelSample.ChannelType, RecordSetProto.RecordSet.Record.ChannelSample> map, String
                            defaultVal) {
                if (map.containsKey(RecordSetProto.RecordSet.Record.ChannelSample.ChannelType.INVERTER_OUTPUT)) {
                    RecordSetProto.RecordSet.Record.ChannelSample channelSample = map.get(RecordSetProto.RecordSet.Record.ChannelSample.ChannelType.INVERTER_OUTPUT);
                    if (channelSample.hasDeviceType()
                            && channelSample.getDeviceType().equals(RecordSetProto.RecordSet.Record.ChannelSample.DeviceType.INVERTER)
                            && channelSample.hasDeviceId()) {
                        return channelSample.getDeviceId();
                    }
                }

                return defaultVal;
            }

            public static String getHost_Rcpn
                    (List<RecordSetProto.RecordSet.Record.ChannelSample> list, String defaultVal) {
                return list.stream()
                        .filter(RecordSetProto.RecordSet.Record.ChannelSample::hasDeviceType)
                        .filter(channelSample -> channelSample.getDeviceType().equals(RecordSetProto.RecordSet.Record.ChannelSample.DeviceType.INVERTER))
                        .filter(RecordSetProto.RecordSet.Record.ChannelSample::hasChannelType)
                        .filter(channelSample -> channelSample.getChannelType().equals(RecordSetProto.RecordSet.Record.ChannelSample.ChannelType.INVERTER_OUTPUT))
                        .findFirst().map(RecordSetProto.RecordSet.Record.ChannelSample::getDeviceId).orElse(defaultVal);
            }

            public static Integer getSystem_mode_or_default(RecordSetProto.RecordSet.Record record, Integer defaultVal) {
                return record.hasSysMode() ? Integer.valueOf(record.getSysMode().getNumber()) : defaultVal;
            }
        }
    }
}
