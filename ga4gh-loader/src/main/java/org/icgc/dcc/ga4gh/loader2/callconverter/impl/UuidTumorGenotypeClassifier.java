package org.icgc.dcc.ga4gh.loader2.callconverter.impl;

import htsjdk.variant.variantcontext.Genotype;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.icgc.dcc.ga4gh.loader2.callconverter.TumorGenotypeClassifier;

@RequiredArgsConstructor
public class UuidTumorGenotypeClassifier implements TumorGenotypeClassifier {

  public static UuidTumorGenotypeClassifier createUuidTumorGenotypeClassifier(String aliquotId) {
    return new UuidTumorGenotypeClassifier(aliquotId);
  }

  @NonNull private final String aliquotId;

  @Override public boolean classify(Genotype genotype) {
    return aliquotId.equals(genotype.getSampleName());
  }

}
