package org.icgc.dcc.ga4gh.loader.utils.idstorage.storage.impl;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.icgc.dcc.ga4gh.loader.utils.idstorage.storage.MapStorage;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import java.io.IOException;
import java.util.Map;

@Slf4j
public class DirectMemoryMapStorage<K,V> implements MapStorage<K,V>{

  public static <K, V> DirectMemoryMapStorage<K, V> createDirectMemoryMapStorage(String name,
      Serializer<K> keySerializer, Serializer<V> valueSerializer,
      MapStorage<K, V> diskMapStorage) {
    return new DirectMemoryMapStorage<K, V>(name, keySerializer, valueSerializer, diskMapStorage);
  }

  @NonNull private final String name;
  @NonNull private final MapStorage<K,V> diskMapStorage;

  private DB db;
  private Map<K,V> memoryMap;

  private DirectMemoryMapStorage(String name, Serializer<K> keySerializer, Serializer<V> valueSerializer,
      MapStorage<K, V> diskMapStorage) {
    this.name = name;
    this.diskMapStorage = diskMapStorage;
    this.db = createDirectMemoryDB();
    this.memoryMap = createDirectMemoryMap(db, name, keySerializer, valueSerializer, diskMapStorage.getMap());
  }


  //create direct memory map storage, and use diskMapStorage as overflow
  @Override public Map<K, V> getMap() {
    return memoryMap;
  }

  private static DB createDirectMemoryDB(){
    return DBMaker
        .memoryDirectDB()
        .closeOnJvmShutdown()
        .concurrencyDisable()
        .make();
  }

  private static <K,V> Map<K,V> createDirectMemoryMap(DB db, String name, Serializer<K> keySerializer, Serializer<V> valueSerializer,  Map<K,V> diskMap ){
    return db
            .hashMap(name, keySerializer, valueSerializer )
            .expireOverflow(diskMap)
            .createOrOpen();
  }

  @Override
  public void close() throws IOException {
    db.close();
    this.diskMapStorage.close();
    log.info("Closed [{}] [{}]", this.name, this.getClass().getSimpleName());
  }

  @Override
  public void purge() {
    try {
      close();
      this.diskMapStorage.purge();
    } catch (IOException e) {
      log.error("Was not able to purge MapStorage: name: {}",name );
    }
  }

}
