package org.icgc.dcc.ga4gh.loader2.factory;

import org.icgc.dcc.ga4gh.common.model.es.EsCall;
import org.icgc.dcc.ga4gh.common.model.es.EsCall.EsCallSerializer;
import org.icgc.dcc.ga4gh.common.model.es.EsCallSet;
import org.icgc.dcc.ga4gh.common.model.es.EsCallSet.EsCallSetSerializer;
import org.icgc.dcc.ga4gh.common.model.es.EsVariant.EsVariantSerializer;
import org.icgc.dcc.ga4gh.common.model.es.EsVariantCallPair2;
import org.icgc.dcc.ga4gh.common.model.es.EsVariantSet;
import org.icgc.dcc.ga4gh.common.model.es.EsVariantSet.EsVariantSetSerializer;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.id.IdStorage;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.id.impl.IdStorageContext;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.id.impl.IdStorageContext.IdStorageContextSerializer;

import static org.mapdb.Serializer.INTEGER;
import static org.mapdb.Serializer.LONG;

public interface IdStorageFactory<T> {

  EsVariantSerializer ES_VARIANT_SERIALIZER = new EsVariantSerializer();
  EsVariantSetSerializer ES_VARIANT_SET_SERIALIZER = new EsVariantSetSerializer();
  EsCallSetSerializer ES_CALL_SET_SERIALIZER = new EsCallSetSerializer();
  EsCallSerializer ES_CALL_SERIALIZER = new EsCallSerializer();
  IdStorageContextSerializer<Integer, EsCall> ID_STORAGE_CONTEXT_INTEGER_SERIALIZER = new IdStorageContextSerializer<>(INTEGER,ES_CALL_SERIALIZER);
  IdStorageContextSerializer<Long, EsCall> ID_STORAGE_CONTEXT_LONG_SERIALIZER = new IdStorageContextSerializer<>( LONG,ES_CALL_SERIALIZER);

  IdStorage<EsVariantCallPair2, IdStorageContext<T, EsCall>> createVariantIdStorage(boolean useDisk);
  IdStorage<EsVariantSet, T> createVariantSetIdStorage(boolean useDisk);
  IdStorage<EsCallSet, T> createCallSetIdStorage(boolean useDisk);
//  IdStorage2<EsVcfHeader, T> createVcfHeaderIdStorage(boolean useDisk);

}
