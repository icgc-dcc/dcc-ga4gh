package org.icgc.dcc.ga4gh.common.model.converters;

import com.fasterxml.jackson.databind.node.ObjectNode;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.VariantContext;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.icgc.dcc.ga4gh.common.JsonNodeConverters;
import org.icgc.dcc.ga4gh.common.PropertyNames;
import org.icgc.dcc.ga4gh.common.model.es.EsVariant;

import java.util.List;
import java.util.Map;

import static org.icgc.dcc.common.core.json.JsonNodeBuilders.object;
import static org.icgc.dcc.ga4gh.common.SearchHits.convertSourceToInteger;
import static org.icgc.dcc.ga4gh.common.SearchHits.convertSourceToString;
import static org.icgc.dcc.ga4gh.common.SearchHits.convertSourceToStringList;

@RequiredArgsConstructor
public class EsVariantConverterJson
    implements JsonObjectNodeConverter<EsVariant>,
    SearchHitConverter<EsVariant> {

  @Override
  public EsVariant convertFromSource(Map<String, Object> source) {
    val start = convertSourceToInteger(source, PropertyNames.START);
    val end = convertSourceToInteger(source, PropertyNames.END);
    val referenceName = convertSourceToString(source, PropertyNames.REFERENCE_NAME);
    val referenceBases = convertSourceToString(source, PropertyNames.REFERENCE_BASES);
    val alternateBases = convertSourceToStringList(source, PropertyNames.ALTERNATIVE_BASES);
    return EsVariant.builder()
        .start(start)
        .end(end)
        .referenceName(referenceName)
        .referenceBases(referenceBases)
        .alternativeBases(alternateBases)
        .build();

  }

  public EsVariant convertFromVariantContext(VariantContext variantContext) {
    val referenceBases = convertAlleleToByteArray(variantContext.getReference());
    val alternativeBases = convertAlleles(variantContext.getAlternateAlleles());
    val start = variantContext.getStart();
    val end = variantContext.getEnd();
    val referenceName = variantContext.getContig();

    return EsVariant.builder()
        .start(start)
        .end(end)
        .referenceName(referenceName)
        .referenceBasesAsBytes(referenceBases)
        .allAlternativeBasesAsBytes(alternativeBases)
        .build();
  }

  @Override
  public ObjectNode convertToObjectNode(EsVariant variant) {
    return object()
        .with(PropertyNames.START, variant.getStart())
        .with(PropertyNames.END, variant.getEnd())
        .with(PropertyNames.REFERENCE_NAME, variant.getReferenceName())
        .with(PropertyNames.REFERENCE_BASES, variant.getReferenceBases())
        .with(PropertyNames.ALTERNATIVE_BASES, JsonNodeConverters.convertStrings(variant.getAlternativeBases()))
        .end();
  }

  public byte[] convertAlleleToByteArray(final Allele allele) {
    return allele.getBases();
  }

  public byte[][] convertAlleles(final List<Allele> alleles) {
    byte[][] bytes = new byte[alleles.size()][];
    int count = 0;
    for (val allele : alleles) {
      bytes[count] = convertAlleleToByteArray(allele);
      count++;
    }
    return bytes;
  }

}
