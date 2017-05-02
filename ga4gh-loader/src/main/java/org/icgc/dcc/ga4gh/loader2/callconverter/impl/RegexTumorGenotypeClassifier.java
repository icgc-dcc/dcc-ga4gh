package org.icgc.dcc.ga4gh.loader2.callconverter.impl;

import htsjdk.variant.variantcontext.Genotype;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.icgc.dcc.ga4gh.loader2.callconverter.TumorGenotypeClassifier;

import java.util.regex.Pattern;

@RequiredArgsConstructor
public class RegexTumorGenotypeClassifier implements TumorGenotypeClassifier {

  public static RegexTumorGenotypeClassifier createRegexTumorGenotypeClassifier(Pattern tumorPattern) {
    return new RegexTumorGenotypeClassifier(tumorPattern);
  }

  @NonNull private final Pattern tumorPattern;

  @Override public boolean classify(Genotype genotype) {
    val tumorSampleName = genotype.getSampleName();
    return tumorPattern.matcher(tumorSampleName).matches();
  }

}
