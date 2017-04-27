package org.icgc.dcc.ga4gh.common.model.converters;

import com.fasterxml.jackson.databind.node.ObjectNode;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.VariantContext;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.elasticsearch.search.SearchHit;
import org.icgc.dcc.ga4gh.common.JsonNodeConverters;
import org.icgc.dcc.ga4gh.common.PropertyNames;
import org.icgc.dcc.ga4gh.common.model.es.EsVariant2;

import java.util.List;
import java.util.Map;

import static org.icgc.dcc.common.core.json.JsonNodeBuilders.object;
import static org.icgc.dcc.ga4gh.common.SearchHits.convertSourceToInteger;
import static org.icgc.dcc.ga4gh.common.SearchHits.convertSourceToString;
import static org.icgc.dcc.ga4gh.common.SearchHits.convertSourceToStringList;

@RequiredArgsConstructor
public class EsVariantConverterJson2
    implements JsonObjectNodeConverter<EsVariant2>,
    SearchHitConverter<EsVariant2> ,
  SourceConverter<EsVariant2> {

  @Override
  public EsVariant2 convertFromSource(Map<String, Object> source) {
    val start = convertSourceToInteger(source, PropertyNames.START);
    val end = convertSourceToInteger(source, PropertyNames.END);
    val referenceName = convertSourceToString(source, PropertyNames.REFERENCE_NAME);
    val referenceBases = convertSourceToString(source, PropertyNames.REFERENCE_BASES);
    val alternateBases = convertSourceToStringList(source, PropertyNames.ALTERNATIVE_BASES);
    return EsVariant2.createEsVariant2()
        .setStart(start)
        .setEnd(end)
        .setReferenceName(referenceName)
        .setReferenceBases(referenceBases)
        .setAlternativeBases(alternateBases);

  }

  @Override
  public EsVariant2 convertFromSearchHit(SearchHit hit) {
    return convertFromSource(hit.getSource());
  }

  public EsVariant2 convertFromVariantContext(VariantContext variantContext) {
    val referenceBases = convertAlleleToByteArray(variantContext.getReference());
    val alternativeBases = convertAlleles(variantContext.getAlternateAlleles());
    val start = variantContext.getStart();
    val end = variantContext.getEnd();
    val referenceName = variantContext.getContig();

    return EsVariant2.createEsVariant2()
        .setStart(start)
        .setEnd(end)
        .setReferenceName(referenceName)
        .setReferenceBases(referenceBases)
        .setAlternativeBases(alternativeBases);
  }

  @Override
  public ObjectNode convertToObjectNode(EsVariant2 variant) {
    return object()
        .with(PropertyNames.START, variant.getStart())
        .with(PropertyNames.END, variant.getEnd())
        .with(PropertyNames.REFERENCE_NAME, variant.getReferenceName())
        .with(PropertyNames.REFERENCE_BASES, variant.getReferenceBasesAsString())
        .with(PropertyNames.ALTERNATIVE_BASES, JsonNodeConverters.convertStrings(variant.getAlternativeBasesAsStrings()))
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
