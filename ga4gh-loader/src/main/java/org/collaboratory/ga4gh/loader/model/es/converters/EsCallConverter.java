package org.collaboratory.ga4gh.loader.model.es.converters;

import static org.collaboratory.ga4gh.core.Names.CALL_SET_ID;
import static org.collaboratory.ga4gh.core.Names.GENOTYPE_LIKELIHOOD;
import static org.collaboratory.ga4gh.core.Names.GENOTYPE_PHASESET;
import static org.collaboratory.ga4gh.core.Names.INFO;
import static org.collaboratory.ga4gh.core.Names.NON_REFERENCE_ALLELES;
import static org.collaboratory.ga4gh.core.Names.VARIANT_SET_ID;
import static org.collaboratory.ga4gh.core.SearchHits.convertHitToBoolean;
import static org.collaboratory.ga4gh.core.SearchHits.convertHitToDouble;
import static org.collaboratory.ga4gh.core.SearchHits.convertHitToInteger;
import static org.collaboratory.ga4gh.core.SearchHits.convertHitToIntegerList;
import static org.collaboratory.ga4gh.core.SearchHits.convertHitToObjectMap;
import static org.collaboratory.ga4gh.loader.utils.JsonNodeConverters.convertIntegers;
import static org.collaboratory.ga4gh.loader.utils.JsonNodeConverters.convertMap;
import static org.icgc.dcc.common.core.json.JsonNodeBuilders.object;

import org.collaboratory.ga4gh.loader.model.es.EsCall;
import org.elasticsearch.search.SearchHit;

import com.fasterxml.jackson.databind.node.ObjectNode;

import lombok.val;

public class EsCallConverter
    implements ObjectNodeConverter<EsCall>,
    SearchHitConverter<EsCall> {

  private static final int DEFAULT_REFERENCE_ALLELE_POSITION = 0;
  private static final int ALTERNATIVE_ALLELE_INDEX_OFFSET = 1;
  private static final int NOT_FOUND_ALLELE_INDEX = -1;

  @Override
  public EsCall convertFromSearchHit(SearchHit hit) {
    val callSetId = convertHitToInteger(hit, CALL_SET_ID);
    val genotypeLikelihood = convertHitToDouble(hit, GENOTYPE_LIKELIHOOD);
    val info = convertHitToObjectMap(hit, INFO);
    val isGenotypePhased = convertHitToBoolean(hit, GENOTYPE_PHASESET);
    val nonReferenceAlleles = convertHitToIntegerList(hit, NON_REFERENCE_ALLELES);
    val variantSetId = convertHitToInteger(hit, VARIANT_SET_ID);

    return EsCall.builder()
        .callSetId(callSetId)
        .genotypeLikelihood(genotypeLikelihood)
        .info(info)
        .isGenotypePhased(isGenotypePhased)
        .nonReferenceAlleles(nonReferenceAlleles)
        .variantSetId(variantSetId)
        .build();

  }

  // rtisma // TODO: Move this back to VCF, no business here
  // rtisma public List<EsCall> convertFromVariantContext(final VariantContext variantContext, final int variantSetId,
  // rtisma final int callSetId) {
  // rtisma val genotypesContext = variantContext.getGenotypes();
  // rtisma val commonInfoMap = variantContext.getCommonInfo().getAttributes();
  // rtisma val altAlleles = variantContext.getAlternateAlleles();
  // rtisma
  // rtisma val callsBuilder = ImmutableList.<EsCall> builder();
  // rtisma for (val genotype : genotypesContext) {
  // rtisma val info = genotype.getExtendedAttributes();
  // rtisma info.putAll(commonInfoMap);
  // rtisma callsBuilder.add(
  // rtisma EsCall.builder()
  // rtisma .variantSetId(variantSetId)
  // rtisma .callSetId(callSetId)
  // rtisma .info(info)
  // rtisma .genotypeLikelihood(genotype.getLog10PError())
  // rtisma .isGenotypePhased(genotype.isPhased())
  // rtisma .nonReferenceAlleles(VCF.convertGenotypeAlleles(altAlleles, genotype))
  // rtisma .build());
  // rtisma }
  // rtisma return callsBuilder.build();
  // rtisma }

  @Override
  public ObjectNode convertToObjectNode(EsCall call) {
    val nonRefAlleles = convertIntegers(call.getNonReferenceAlleles());
    return object()
        .with(VARIANT_SET_ID, call.getVariantSetId())
        .with(CALL_SET_ID, call.getCallSetId())
        .with(INFO, convertMap(call.getInfo()))
        .with(GENOTYPE_LIKELIHOOD, Double.toString(call.getGenotypeLikelihood()))
        .with(GENOTYPE_PHASESET, call.isGenotypePhased())
        .with(NON_REFERENCE_ALLELES, nonRefAlleles)
        .end();
  }

}
