package org.icgc.dcc.ga4gh.loader2.callconverter;

import htsjdk.variant.variantcontext.Genotype;

public interface TumorGenotypeClassifier {

  boolean classify(Genotype genotype);


}
