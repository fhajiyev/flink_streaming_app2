package com.neurio.app.dto;

import lombok.Data;

@Data
public class SystemEnergyDto {
    private Raw raw =  new Raw();

    @Data
    public static class Raw {
        private RawData inverter = new RawData();

        @Data
        public static class RawData {
            private Double lifeTimeImported_Ws = 0.0d;
            private Double lifeTimeExported_Ws = 0.0d;
            private Double currentImport_Ws = 0.0d;
            private Double currentExport_Ws = 0.0d;
        }
    }
}