package org.icgc.dcc.ga4gh.loader2;

import htsjdk.variant.variantcontext.VariantContext;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.val;
import org.icgc.dcc.ga4gh.common.model.converters.EsVariantConverterJson2;
import org.icgc.dcc.ga4gh.common.model.es.EsCall;
import org.icgc.dcc.ga4gh.common.model.es.EsCall.EsCallBuilder;
import org.icgc.dcc.ga4gh.common.model.es.EsCallSet;
import org.icgc.dcc.ga4gh.common.model.es.EsVariant2;
import org.icgc.dcc.ga4gh.common.model.es.EsVariantSet;
import org.icgc.dcc.ga4gh.common.model.portal.PortalMetadata;
import org.icgc.dcc.ga4gh.loader.utils.CounterMonitor;
import org.icgc.dcc.ga4gh.loader2.callconverter.CallConverterStrategy;
import org.icgc.dcc.ga4gh.loader2.callconverter.CallConverterStrategyMux;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.id.IdStorage;

import java.io.File;

import static org.icgc.dcc.common.core.util.stream.Streams.stream;
import static org.icgc.dcc.ga4gh.common.model.converters.EsVariantSetConverterJson.convertFromPortalMetadata;
import static org.icgc.dcc.ga4gh.loader2.utils.VCF.newDefaultVCFFileReader;

/**
 * Processes a vcf file by creating EsVariant objects using the selected CallConverterStrategy, and populates the
 * variantIdStorage object with all the streamed variants
 */
@RequiredArgsConstructor
@Value
public class VcfProcessor {

  private static final CallConverterStrategyMux CALL_CONVERTER_STRATEGY_MUX = new CallConverterStrategyMux();
  private static final EsVariantConverterJson2 ES_VARIANT_CONVERTER_JSON_2 = new EsVariantConverterJson2();

  public static VcfProcessor createVcfProcessor(IdStorage<EsVariant2, Long> variantIdStorage,
      IdStorage<EsVariantSet, Integer> variantSetIdStorage,
      IdStorage<EsCallSet, Integer> callSetIdStorage,
      CallSetDao callSetDao,
      CounterMonitor callCounterMonitor){
    return new VcfProcessor(variantIdStorage, variantSetIdStorage, callSetIdStorage, callSetDao, callCounterMonitor);

  }

  @NonNull private final IdStorage<EsVariant2, Long> variantIdStorage;
  @NonNull private final IdStorage<EsVariantSet, Integer> variantSetIdStorage;
  @NonNull private final IdStorage<EsCallSet, Integer> callSetIdStorage;
  @NonNull private final CallSetDao callSetDao;
  @NonNull private final CounterMonitor callCounterMonitor;

  public void process(PortalMetadata portalMetadata, File vcfFile){
    val vcfFileReader = newDefaultVCFFileReader(vcfFile);
    val callConverter = CALL_CONVERTER_STRATEGY_MUX.select(portalMetadata);
    val esCallBuilder = createEsCallBuilder(portalMetadata);

    stream(vcfFileReader).forEach(v -> processVariant(callConverter, esCallBuilder, v));
  }

  /**
   * Creates a prefilled EsCallBuilder so that can be used as efficiently as possible
   * when creating many EsCall objects. Since there can be millions of EsCall objects generated
   * per file, the only EsCall information that does not change in the scope of a vcf file
   * are the callSetName, callSetId and variantSetId, hence why this method exists. Uses PortalMetadata
   * objects to query the CallSetDao
   */
  private EsCallBuilder createEsCallBuilder(PortalMetadata portalMetadata){
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

  private void processVariant(CallConverterStrategy callConverterStrategy, EsCallBuilder esCallBuilder, VariantContext variantContext){
    val esCalls = callConverterStrategy.convert(esCallBuilder, variantContext);
    val esVariant2 = ES_VARIANT_CONVERTER_JSON_2.convertFromVariantContext(variantContext);
    esCalls.forEach(esVariant2::addCall);
    variantIdStorage.add(esVariant2);
    callCounterMonitor.incr();
  }

}
