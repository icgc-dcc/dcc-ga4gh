package org.icgc.dcc.ga4gh.loader.utils.idstorage.id.impl;

import lombok.NonNull;
import lombok.Value;
import org.icgc.dcc.ga4gh.common.model.es.EsVariantCallPair;

@Value
public class VariantIdContext<ID> {

  @NonNull private final ID id;
  @NonNull private final EsVariantCallPair esVariantCallPair;

  public static <ID> VariantIdContext<ID> createVariantIdContext(ID id, EsVariantCallPair esVariantCallPair) {
    return new VariantIdContext<ID>(id, esVariantCallPair);
  }

}
