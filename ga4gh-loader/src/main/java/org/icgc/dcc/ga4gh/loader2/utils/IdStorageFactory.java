package org.icgc.dcc.ga4gh.loader2.utils;

import org.icgc.dcc.ga4gh.common.model.es.EsCallSet;
import org.icgc.dcc.ga4gh.common.model.es.EsVariant2;
import org.icgc.dcc.ga4gh.common.model.es.EsVariantSet;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.id.IdStorage;

public interface IdStorageFactory<T> {

  IdStorage<EsVariant2, T> createVariantIdStorage(boolean useDisk);
  IdStorage<EsVariantSet, T> createVariantSetIdStorage(boolean useDisk);
  IdStorage<EsCallSet, T> createCallSetIdStorage(boolean useDisk);

}
