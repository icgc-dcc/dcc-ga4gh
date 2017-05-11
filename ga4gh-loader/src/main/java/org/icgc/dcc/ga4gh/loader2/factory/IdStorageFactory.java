package org.icgc.dcc.ga4gh.loader2.factory;

import org.icgc.dcc.ga4gh.common.model.es.EsCall;
import org.icgc.dcc.ga4gh.common.model.es.EsCallSet;
import org.icgc.dcc.ga4gh.common.model.es.EsVariantCallPair2;
import org.icgc.dcc.ga4gh.common.model.es.EsVariantSet;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.context.IdStorageContext;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.id.IdStorage;

public interface IdStorageFactory<T> {


  IdStorage<EsVariantCallPair2, IdStorageContext<T, EsCall>> createVariantIdStorage(boolean useDisk);
  IdStorage<EsVariantSet, T> createVariantSetIdStorage(boolean useDisk);
  IdStorage<EsCallSet, T> createCallSetIdStorage(boolean useDisk);

  IdStorage<EsVariantCallPair2, IdStorageContext<T, EsCall>> persistVariantIdStorage();
  IdStorage<EsVariantSet, T> persistVariantSetIdStorage();
  IdStorage<EsCallSet, T> persistCallSetIdStorage();

//  IdStorage2<EsVcfHeader, T> createVcfHeaderIdStorage(boolean useDisk);

}
