package org.icgc.dcc.ga4gh.loader;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import org.icgc.dcc.ga4gh.common.model.es.EsVariant;

@Value
@Builder
public class VariantIdContext {

  @NonNull
  private final String dataType;

  @NonNull
  private final EsVariant variant;

}
