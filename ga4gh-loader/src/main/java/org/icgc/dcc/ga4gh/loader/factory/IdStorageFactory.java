package org.icgc.dcc.ga4gh.loader.factory;

import org.icgc.dcc.ga4gh.common.model.es.EsCallSet;
import org.icgc.dcc.ga4gh.common.model.es.EsVariantSet;
import org.icgc.dcc.ga4gh.loader.utils.idstorage.id.AbstractIdStorageTemplate;

public interface IdStorageFactory<T extends Number> {

  AbstractIdStorageTemplate<EsVariantSet, T> createVariantSetIdStorage(boolean useDisk);
  AbstractIdStorageTemplate<EsCallSet, T> createCallSetIdStorage(boolean useDisk);

  AbstractIdStorageTemplate<EsVariantSet, T> persistVariantSetIdStorage();
  AbstractIdStorageTemplate<EsCallSet, T> persistCallSetIdStorage();

}
