package org.icgc.dcc.ga4gh.loader.callconverter.impl;

import htsjdk.variant.variantcontext.Genotype;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.icgc.dcc.ga4gh.loader.callconverter.TumorGenotypeClassifier;

import java.util.function.BiFunction;

@RequiredArgsConstructor
public class FunctorTumorGenotypeClassifier implements TumorGenotypeClassifier {

  public static FunctorTumorGenotypeClassifier createFunctorTumorGenotypeClassifier(String value, BiFunction<String, String, Boolean> functor) {
    return new FunctorTumorGenotypeClassifier(value, functor);
  }

  @NonNull private final String value;
  @NonNull private final BiFunction<String, String, Boolean> functor;

  @Override public boolean classify(Genotype genotype) {
    return functor.apply(value, genotype.getSampleName());
  }

}
