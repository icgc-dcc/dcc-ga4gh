package org.icgc.dcc.ga4gh.loader.utils.idstorage.id.impl;

import lombok.NoArgsConstructor;
import lombok.val;
import org.icgc.dcc.ga4gh.common.model.es.EsCallSet;
import org.icgc.dcc.ga4gh.common.model.es.EsVariantSet;
import org.icgc.dcc.ga4gh.loader.utils.idstorage.id.AbstractIdStorageTemplate;

import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.ga4gh.loader.Config.VARIANT_MAPDB_ALLOCATION;
import static org.icgc.dcc.ga4gh.loader.factory.Factory.ES_CONSENSUS_CALL_LIST_SERIALIZER;
import static org.icgc.dcc.ga4gh.loader.factory.Factory.ES_CALL_SET_SERIALIZER;
import static org.icgc.dcc.ga4gh.loader.factory.Factory.ES_VARIANT_SERIALIZER;
import static org.icgc.dcc.ga4gh.loader.factory.Factory.ES_VARIANT_SET_SERIALIZER;
import static org.icgc.dcc.ga4gh.loader.factory.Factory.RESOURCE_PERSISTED_PATH;
import static org.icgc.dcc.ga4gh.loader.utils.idstorage.id.impl.IntegerIdStorage.createIntegerIdStorage;
import static org.icgc.dcc.ga4gh.loader.utils.idstorage.id.impl.VariantAggregator.createVariantAggregator;
import static org.icgc.dcc.ga4gh.loader.utils.idstorage.storage.MapStorageFactory.createMapStorageFactory;
import static org.mapdb.Serializer.INTEGER;

@NoArgsConstructor(access = PRIVATE)
public class IdStorageFactory2 {

  //-XX:MaxDirectMemorySize=10G
  public static AbstractIdStorageTemplate<EsVariantSet, Integer>  buildVariantSetIdStorage(){
    val mapStorageFactory = createMapStorageFactory("variantSetIntegerMapStorage",
        ES_VARIANT_SET_SERIALIZER, INTEGER,
        RESOURCE_PERSISTED_PATH, -1);
    val mapStorage = mapStorageFactory.createDiskMapStorage(false);
    return createIntegerIdStorage(mapStorage, 0);
  }

  public static AbstractIdStorageTemplate<EsCallSet, Integer>  buildCallSetIdStorage(){
    val mapStorageFactory = createMapStorageFactory("callSetIntegerMapStorage",
        ES_CALL_SET_SERIALIZER, INTEGER,
        RESOURCE_PERSISTED_PATH, -1);
    val mapStorage = mapStorageFactory.createDiskMapStorage(false);
    return createIntegerIdStorage(mapStorage, 0);
  }

  public static VariantAggregator  buildVariantAggregator(){
    val mapStorageFactory = createMapStorageFactory("variantLongMapStorage",
        ES_VARIANT_SERIALIZER, ES_CONSENSUS_CALL_LIST_SERIALIZER,
        RESOURCE_PERSISTED_PATH, VARIANT_MAPDB_ALLOCATION);
    val mapStorage = mapStorageFactory.createDirectMemoryMapStorage(true);
    return createVariantAggregator(mapStorage);
  }

}
