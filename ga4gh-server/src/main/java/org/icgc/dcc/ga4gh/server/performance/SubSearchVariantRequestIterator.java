package org.icgc.dcc.ga4gh.server.performance;

import ga4gh.VariantServiceOuterClass;
import lombok.NonNull;
import lombok.val;

import java.util.Iterator;

public class SubSearchVariantRequestIterator
    implements Iterator<VariantServiceOuterClass.SearchVariantsRequest.Builder> {

  public static SubSearchVariantRequestIterator createSubSearchVariantRequestIterator(
      String referenceName, int minStart, int maxEnd, int variantLength) {
    return new SubSearchVariantRequestIterator(referenceName, minStart, maxEnd,
        variantLength);
  }

  @NonNull private final String referenceName;
  private final int minStart;
  private final int maxEnd;
  private final int variantLength;


  private int currentStart;

  public SubSearchVariantRequestIterator(String referenceName, int minStart, int maxEnd, int variantLength) {
    this.referenceName = referenceName;
    this.minStart = minStart;
    this.maxEnd = maxEnd;
    this.variantLength = variantLength;
    this.currentStart = minStart;
  }

  public int getSize(){
    return (int)Math.floor((maxEnd - variantLength - minStart)/(double)variantLength);
  }


  @Override public boolean hasNext() {
    return currentStart + variantLength < maxEnd;
  }

  @Override public VariantServiceOuterClass.SearchVariantsRequest.Builder next() {
    val builder = VariantServiceOuterClass.SearchVariantsRequest.newBuilder()
        .setReferenceName(referenceName)
        .setEnd(currentStart + variantLength)
        .setStart(currentStart);
    currentStart += variantLength;
    return builder;
  }

}
