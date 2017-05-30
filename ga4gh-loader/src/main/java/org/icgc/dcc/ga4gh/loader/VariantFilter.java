package org.icgc.dcc.ga4gh.loader;

import htsjdk.variant.variantcontext.VariantContext;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class VariantFilter {

  private final boolean bypassFilter;

  public boolean passedFilter(VariantContext variantContext){
    if (bypassFilter){
      return true;
    } else {
      return variantContext.isNotFiltered();
    }
  }

  public boolean notPassedFilter(VariantContext variantContext){
    return ! passedFilter(variantContext);
  }

  public static VariantFilter createVariantFilter(boolean bypassFilter) {
    return new VariantFilter(bypassFilter);
  }

}
