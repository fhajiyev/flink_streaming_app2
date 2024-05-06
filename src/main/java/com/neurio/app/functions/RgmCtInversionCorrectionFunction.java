package com.neurio.app.functions;

import com.generac.ces.essdataprovider.model.PagedResponseDto;
import com.neurio.app.api.services.essdp.EssDpService;
import com.neurio.app.api.services.essdp.EssDpServiceStub;
import com.neurio.app.api.services.essdp.IEssDpService;
import com.neurio.app.config.AppConfig;
import com.neurio.app.dataframe.DataFrame;
import com.neurio.app.dto.SystemEnergyDto;
import lombok.Data;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.net.http.HttpClient;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Slf4j
@Data
public class RgmCtInversionCorrectionFunction extends KeyedProcessFunction<String, DataFrame, DataFrame> {

    private AppConfig appConfig;
    private int essCheckIntervalHours = 6;
    private int TIME_DIFF_IN_SECONDS = 300;
    private transient ValueState<Tuple2<Boolean, Long>> previousIsSwappedState;

    @Setter
    private transient IEssDpService essDpService;


    @Override
    public void processElement(DataFrame df, Context ctx, Collector<DataFrame> collector) throws Exception {

        long rawRgmImported = df.getEnergyMetaData().getRaw().getRaw_inverter_lifetime_imported_Ws();
        long rawRgmExported = df.getEnergyMetaData().getRaw().getRaw_inverter_lifetime_exported_Ws();

        // we cannot determine if we need to swap or not if rgm delta import and export values are equal
        if(rawRgmImported == rawRgmExported) {
            df.getEnergyMetaData().setIsInverterOutputSwapped(false);
            collector.collect(df);
            return;
        }

        long currDfTimestamp = df.getTimestamp_utc() + 1;
        long prevDfTimestamp = currDfTimestamp - TIME_DIFF_IN_SECONDS;

        // check ess and update state
        if(isValidForEssCheck(currDfTimestamp)) {

            long validUpTo = currDfTimestamp + TimeUnit.HOURS.toSeconds(essCheckIntervalHours); // next time to check
            try {

                List<SystemEnergyDto> list = getEssDpResponse(df.getSystemMetaData().getSystemId(), prevDfTimestamp, currDfTimestamp);

                if (list.size() > 0) {

                    SystemEnergyDto dto = list.get(0);
                    double rawEssImported = dto.getRaw().getInverter().getLifeTimeImported_Ws();
                    double rawEssExported = dto.getRaw().getInverter().getLifeTimeExported_Ws();

                    // we cannot determine if we need to swap or not if ess delta import and export values are equal
                    if(rawEssImported == rawEssExported) {
                        df.getEnergyMetaData().setIsInverterOutputSwapped(false);
                        collector.collect(df);
                        return;
                    }

                    if (rawEssImported - rawEssExported < 0 && rawRgmImported - rawRgmExported > 0 ||
                            rawEssImported - rawEssExported > 0 && rawRgmImported - rawRgmExported < 0) {
                        //log.warn("swap needed for system id: {}. deltaEssImp-deltaEssExp = {} | deltaRgmImp-deltaRgmExp = {}", df.getSystemMetaData().getSystemId(), deltaEssImported - deltaEssExported, deltaRgmImported - deltaRgmExported);
                        updateState(true, validUpTo);
                    }
                    else {
                        //log.warn("swap not needed for system id: {}. deltaEssImp-deltaEssExp = {} | deltaRgmImp-deltaRgmExp = {}", df.getSystemMetaData().getSystemId(), deltaEssImported - deltaEssExported, deltaRgmImported - deltaRgmExported);
                        updateState(false, validUpTo);
                    }

                } else { // no ess record found
                    if(getState()==null) {
                        updateState(false, validUpTo); // if no state present then update to "do not swap" otherwise use existing state
                    }
                }

            } catch (Exception e) { // exception occurred in the middle
                log.error("Exception occurred on ess-dp request: {} | systemId: {}", e.getMessage(), df.getSystemMetaData().getSystemId());
                if(getState()==null) {
                    updateState(false, validUpTo); // if no state present then update to "do not swap" otherwise use existing state
                }
            }
        }

        // swap if needed and send updated dataframe to collector
        if(getIsSwapped()) {
            swapImpExp(df);
        }
        else {
            df.getEnergyMetaData().setIsInverterOutputSwapped(false);
        }

        collector.collect(df);
    }

    private boolean isValidForEssCheck(Long currTimestamp) throws Exception {

        if(previousIsSwappedState.value() == null) {
            return true;
        }

        if(currTimestamp < getValidUpTo())
            return false;

        return true;
    }

    public RgmCtInversionCorrectionFunction(AppConfig appConfig) {
        this.appConfig = appConfig;
    }

    @VisibleForTesting
    private void setEssDpService(IEssDpService essDpService) {
        this.essDpService = essDpService;
    }

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Tuple2<Boolean, Long>> recordStateDescriptor =
                new ValueStateDescriptor<>(
                        "previous record keyed by systemid", // the state name
                        TypeInformation.of(new TypeHint<Tuple2<Boolean, Long>>() {
                        }) // type information
                );

        this.previousIsSwappedState = getRuntimeContext().getState(recordStateDescriptor);

        // initialize ess-dp service
        final HttpClient httpClient = HttpClient.newBuilder().build();
        if (appConfig.getEssDpServiceApiConfig().isUseStub()) {
            // use service stubs
            if(essDpService == null) // this check is needed in order not to overwrite mock stub
                essDpService = new EssDpServiceStub();
        } else {
            essDpService = new EssDpService(httpClient, appConfig.getEssDpServiceApiConfig());
        }
    }

    private List<SystemEnergyDto> getEssDpResponse(String systemId, Long prevTimestamp, Long currTimestamp) throws InterruptedException, ExecutionException {

        PagedResponseDto<SystemEnergyDto> dto = essDpService.getEssEnergyByStartAndEndTimes(systemId, prevTimestamp, currTimestamp).get();

        return dto.getData();
    }

    private void swapImpExp(DataFrame df) {

        // swap raws
        long rawImp = df.getEnergyMetaData().getRaw().getRaw_inverter_lifetime_imported_Ws();
        long rawExp = df.getEnergyMetaData().getRaw().getRaw_inverter_lifetime_exported_Ws();

        df.getEnergyMetaData().getRaw().setRaw_inverter_lifetime_imported_Ws(rawExp);
        df.getEnergyMetaData().getRaw().setRaw_inverter_lifetime_exported_Ws(rawImp);

        // swap deltas
        long deltaImp = df.getEnergyMetaData().getEnergyDelta().getInverter_energy_imported_delta_Ws();
        long deltaExp = df.getEnergyMetaData().getEnergyDelta().getInverter_energy_exported_delta_Ws();

        df.getEnergyMetaData().getEnergyDelta().setInverter_energy_imported_delta_Ws(deltaExp);
        df.getEnergyMetaData().getEnergyDelta().setInverter_energy_exported_delta_Ws(deltaImp);

        df.getEnergyMetaData().setIsInverterOutputSwapped(true);
    }

    private void updateState(boolean isSwapped, Long timestamp) throws Exception {
        previousIsSwappedState.update(new Tuple2<>(isSwapped, timestamp));
    }

    private Tuple2<Boolean, Long> getState() throws Exception {
        return previousIsSwappedState.value();
    }

    private boolean getIsSwapped() throws Exception {
        return previousIsSwappedState.value().f0;
    }

    private long getValidUpTo() throws Exception {
        return previousIsSwappedState.value().f1;
    }
}