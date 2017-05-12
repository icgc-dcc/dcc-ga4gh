package org.icgc.dcc.ga4gh.loader2.factory;

import org.icgc.dcc.ga4gh.common.model.es.EsCallSet;
import org.icgc.dcc.ga4gh.common.model.es.EsVariantSet;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.id.AbstractIdStorageTemplate;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.id.impl.VariantIdStorage;

public interface IdStorageFactory<T extends Number> {


  VariantIdStorage<T> createVariantIdStorage(boolean useDisk);
  AbstractIdStorageTemplate<EsVariantSet, T> createVariantSetIdStorage(boolean useDisk);
  AbstractIdStorageTemplate<EsCallSet, T> createCallSetIdStorage(boolean useDisk);

  VariantIdStorage<T> persistVariantIdStorage();
  AbstractIdStorageTemplate<EsVariantSet, T> persistVariantSetIdStorage();
  AbstractIdStorageTemplate<EsCallSet, T> persistCallSetIdStorage();

//  IdStorage2<EsVcfHeader, T> createVcfHeaderIdStorage(boolean useDisk);

}
