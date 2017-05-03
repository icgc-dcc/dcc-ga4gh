package org.icgc.dcc.ga4gh.loader2;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFileReader;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.icgc.dcc.ga4gh.common.model.converters.EsVariantConverterJson2;
import org.icgc.dcc.ga4gh.common.model.es.EsCall;
import org.icgc.dcc.ga4gh.common.model.es.EsCallSet;
import org.icgc.dcc.ga4gh.common.model.es.EsVariant2;
import org.icgc.dcc.ga4gh.common.model.es.EsVariantSet;
import org.icgc.dcc.ga4gh.common.model.portal.PortalMetadata;
import org.icgc.dcc.ga4gh.loader.utils.CounterMonitor;
import org.icgc.dcc.ga4gh.loader2.callconverter.CallConverterStrategy;
import org.icgc.dcc.ga4gh.loader2.callconverter.CallConverterStrategyMux;
import org.icgc.dcc.ga4gh.loader2.utils.VCF;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.id.IdStorage;

import java.io.File;
import java.util.stream.Stream;

import static org.icgc.dcc.common.core.util.stream.Streams.stream;
import static org.icgc.dcc.ga4gh.common.model.converters.EsVariantSetConverterJson.convertFromPortalMetadata;

@RequiredArgsConstructor
public class VcfProcessor {
  private static final CallConverterStrategyMux CALL_CONVERTER_STRATEGY_MUX = new CallConverterStrategyMux();

  private static final EsVariantConverterJson2 ES_VARIANT_CONVERTER_JSON_2 = new EsVariantConverterJson2();

  private static EsCall.EsCallBuilder createEsCallBuilder(PortalMetadata portalMetadata,
        IdStorage<EsVariantSet, Integer> variantSetIdStorage,
        IdStorage<EsCallSet, Integer> callSetIdStorage,
        CallSetDao callSetDao){
    val variantSet = convertFromPortalMetadata(portalMetadata);
    val variantSetId = variantSetIdStorage.getId(variantSet);

    val callSet = callSetDao.find(portalMetadata).get();
    val callSetName = callSet.getName();
    val callSetId = callSetIdStorage.getId(callSet);

    return EsCall.builder()
        .callSetName(callSetName)
        .callSetId(callSetId)
        .variantSetId(variantSetId);
  }

  public static VcfProcessor createVcfProcessor(PortalMetadata portalMetadata,
      File vcfFile,
      IdStorage<EsVariant2, Long> variantIdStorage,
      IdStorage<EsVariantSet, Integer> variantSetIdStorage,
      IdStorage<EsCallSet, Integer> callSetIdStorage,
      CallSetDao callSetDao,
      CounterMonitor callCounterMonitor){
    val callConverter = CALL_CONVERTER_STRATEGY_MUX.select(portalMetadata);
    val esCallBuilder = createEsCallBuilder(portalMetadata,
        variantSetIdStorage,callSetIdStorage,callSetDao);
    val vcfFileReader = VCF.newDefaultVCFFileReader(vcfFile);
    return new VcfProcessor(vcfFileReader, esCallBuilder, callConverter, variantIdStorage, callCounterMonitor);

  }

  @NonNull private final VCFFileReader vcfFileReader;
  @NonNull private final EsCall.EsCallBuilder esCallBuilder;
  @NonNull private final CallConverterStrategy callConverterStrategy;
  @NonNull private final IdStorage<EsVariant2, Long> variantIdStorage;
  @NonNull private final CounterMonitor callCounterMonitor;

  public Stream<EsVariant2> streamVariants(){
    return stream(vcfFileReader)
        .map(this::convertVariant);
  }

  private EsVariant2 convertVariant(VariantContext variantContext){
    val esCalls = callConverterStrategy.convert(esCallBuilder, variantContext);
    val esVariant2 = ES_VARIANT_CONVERTER_JSON_2.convertFromVariantContext(variantContext);
    callCounterMonitor.incr(esCalls.size());
    esCalls.forEach(esVariant2::addCall);
    variantIdStorage.add(esVariant2);
    return esVariant2;
  }

}
