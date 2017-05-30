package org.icgc.dcc.ga4gh.loader.utils;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import java.nio.file.Path;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;
import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.ga4gh.loader.utils.CheckPaths.checkDirPath;

@RequiredArgsConstructor(access = PRIVATE)
public class MapFactory<K,V> {

  @NonNull private final String name;
  @NonNull private final Path dirname;
  @NonNull private final Serializer<K> keySerializer;
  @NonNull private final Serializer<V> valueSerializer;
  private final long allocation;


  public Path getFilename(){
    return dirname.resolve(name+".db");
  }

  public Map<K,V> createRamMap(){
    return newHashMap();
  }

  public  Map<K, V> createConcurrentMemoryMap() {
    val diskMap = createDiskMap();
      return DBMaker
          .memoryShardedHashMap(8)
          .valueSerializer(valueSerializer)
          .keySerializer(keySerializer)
          .layout(8, 128, 490000)
          .expireOverflow(diskMap)
          .createOrOpen();
  }

  public Map<K, V> createDiskMap() {
    return DBMaker
            .fileDB(getFilename().toString())
            .concurrencyDisable()
            .closeOnJvmShutdown()
            .allocateIncrement(allocation) //TODO: rtisma_20170511_hack
            .allocateStartSize(allocation) //TODO: rtisma_20170511_hack
          .make()
        .hashMap(name, keySerializer, valueSerializer)
        .createOrOpen();
  }

  public static <K, V> MapFactory<K, V> createMapDBFactory(String name, Path dirname, Serializer<K> keySerializer,
      Serializer<V> valueSerializer,
      long allocation) {
    checkDirPath(dirname);
    return new MapFactory<K, V>(name, dirname, keySerializer, valueSerializer, allocation);
  }

}
