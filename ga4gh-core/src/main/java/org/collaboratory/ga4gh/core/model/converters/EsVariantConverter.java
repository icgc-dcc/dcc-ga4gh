package org.collaboratory.ga4gh.core.model.converters;

import com.fasterxml.jackson.databind.node.ObjectNode;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.VariantContext;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.collaboratory.ga4gh.core.model.es.EsVariant;
import org.elasticsearch.search.SearchHit;

import java.util.List;

import static org.collaboratory.ga4gh.core.JsonNodeConverters.convertStrings;
import static org.collaboratory.ga4gh.core.PropertyNames.ALTERNATIVE_BASES;
import static org.collaboratory.ga4gh.core.PropertyNames.END;
import static org.collaboratory.ga4gh.core.PropertyNames.REFERENCE_BASES;
import static org.collaboratory.ga4gh.core.PropertyNames.REFERENCE_NAME;
import static org.collaboratory.ga4gh.core.PropertyNames.START;
import static org.collaboratory.ga4gh.core.SearchHits.convertHitToInteger;
import static org.collaboratory.ga4gh.core.SearchHits.convertHitToString;
import static org.collaboratory.ga4gh.core.SearchHits.convertHitToStringList;
import static org.icgc.dcc.common.core.json.JsonNodeBuilders.object;

@RequiredArgsConstructor
public class EsVariantConverter
    implements ObjectNodeConverter<EsVariant>,
    SearchHitConverter<EsVariant> {

  @Override
  public EsVariant convertFromSearchHit(SearchHit hit) {
    val start = convertHitToInteger(hit, START);
    val end = convertHitToInteger(hit, END);
    val referenceName = convertHitToString(hit, REFERENCE_NAME);
    val referenceBases = convertHitToString(hit, REFERENCE_BASES);
    val alternateBases = convertHitToStringList(hit, ALTERNATIVE_BASES);
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
        .with(START, variant.getStart())
        .with(END, variant.getEnd())
        .with(REFERENCE_NAME, variant.getReferenceName())
        .with(REFERENCE_BASES, variant.getReferenceBases())
        .with(ALTERNATIVE_BASES, convertStrings(variant.getAlternativeBases()))
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
