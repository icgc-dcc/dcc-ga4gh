package org.icgc.dcc.ga4gh.common.model.converters;

import com.fasterxml.jackson.databind.node.ObjectNode;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.VariantContext;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.icgc.dcc.ga4gh.common.model.es.EsVariant2Impl;

import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;
import static org.icgc.dcc.common.core.json.JsonNodeBuilders.object;
import static org.icgc.dcc.ga4gh.common.JsonNodeConverters.convertJsonNodes;
import static org.icgc.dcc.ga4gh.common.JsonNodeConverters.convertStrings;
import static org.icgc.dcc.ga4gh.common.PropertyNames.ALTERNATIVE_BASES;
import static org.icgc.dcc.ga4gh.common.PropertyNames.CALLS;
import static org.icgc.dcc.ga4gh.common.PropertyNames.END;
import static org.icgc.dcc.ga4gh.common.PropertyNames.REFERENCE_BASES;
import static org.icgc.dcc.ga4gh.common.PropertyNames.REFERENCE_NAME;
import static org.icgc.dcc.ga4gh.common.PropertyNames.START;
import static org.icgc.dcc.ga4gh.common.SearchHits.convertSourceToInteger;
import static org.icgc.dcc.ga4gh.common.SearchHits.convertSourceToObjectList;
import static org.icgc.dcc.ga4gh.common.SearchHits.convertSourceToString;
import static org.icgc.dcc.ga4gh.common.SearchHits.convertSourceToStringList;

@RequiredArgsConstructor
public class EsVariantConverterJson2
    implements JsonObjectNodeConverter<EsVariant2Impl>,
    SearchHitConverter<EsVariant2Impl> {

  private static final EsCallConverterJson ES_CALL_CONVERTER_JSON = new EsCallConverterJson();

  @Override
  public EsVariant2Impl convertFromSource(Map<String, Object> source) {
    val start = convertSourceToInteger(source, START);
    val end = convertSourceToInteger(source, END);
    val referenceName = convertSourceToString(source, REFERENCE_NAME);
    val referenceBases = convertSourceToString(source, REFERENCE_BASES);
    val alternateBases = convertSourceToStringList(source, ALTERNATIVE_BASES);
    val callObjectNodes = convertSourceToObjectList(source, CALLS);
    //TODO: implement calls creation
    return EsVariant2Impl.createEsVariant2()
        .setStart(start)
        .setEnd(end)
        .setReferenceName(referenceName)
        .setReferenceBases(referenceBases)
        .setAlternativeBases(alternateBases);

  }

  public EsVariant2Impl convertFromVariantContext(VariantContext variantContext) {
    val referenceBases = convertAlleleToByteArray(variantContext.getReference());
    val alternativeBases = convertAlleles(variantContext.getAlternateAlleles());
    val start = variantContext.getStart();
    val end = variantContext.getEnd();
    val referenceName = variantContext.getContig();

    return EsVariant2Impl.createEsVariant2()
        .setStart(start)
        .setEnd(end)
        .setReferenceName(referenceName)
        .setReferenceBases(referenceBases)
        .setAlternativeBases(alternativeBases);
  }

  @Override
  public ObjectNode convertToObjectNode(EsVariant2Impl variant) {
    val calls = variant.getCalls().stream().map(ES_CALL_CONVERTER_JSON::convertToObjectNode).collect(toList());
    return object()
        .with(START, variant.getStart())
        .with(END, variant.getEnd())
        .with(REFERENCE_NAME, variant.getReferenceName())
        .with(REFERENCE_BASES, variant.getReferenceBasesAsString())
        .with(ALTERNATIVE_BASES, convertStrings(variant.getAlternativeBasesAsStrings()))
        .with(CALLS, convertJsonNodes(calls))
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
