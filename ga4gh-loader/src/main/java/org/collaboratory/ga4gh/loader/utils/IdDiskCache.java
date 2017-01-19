package org.collaboratory.ga4gh.loader.utils;

import java.io.File;
import java.util.Map;

import org.icgc.dcc.common.core.util.Joiners;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import lombok.val;

public class IdDiskCache implements IdCache<String>, AutoCloseable {

  // Input dep
  private final String name;
  private final String outputDirname;

  // Internal Deps
  private final DB db;
  private final IdCache<String> idCache;

  public IdDiskCache(String name, String outputDirname) {
    this(name, outputDirname, 0L);
  }

  public IdDiskCache(String name, String outputDirname, Long id) {
    this.name = name;
    this.outputDirname = outputDirname;
    val filename = Joiners.PATH.join(outputDirname, name + ".db");
    this.db = createEntityDB(filename);
    val map = newEntityMap(db, name);
    this.idCache = IdCacheImpl.newIdCache(map, id);
  }

  private static DB createEntityDB(String filename) {
    val file = new File(filename); // TODO: add better file management here
    return DBMaker
        .fileDB(file)
        .concurrencyDisable()
        .fileMmapEnable()
        .make();
  }

  private static Map<String, Long> newEntityMap(DB db, String name) {
    return db
        .hashMap(name, Serializer.STRING_ASCII, Serializer.LONG)
        .createOrOpen();
  }

  @Override
  public void add(String t) {
    idCache.add(t);
  }

  @Override
  public boolean contains(String t) {
    return idCache.contains(t);
  }

  @Override
  public String getIdAsString(String t) {
    return idCache.getIdAsString(t);
  }

  @Override
  public Long getId(String t) {
    return idCache.getId(t);
  }

  @Override
  public void close() throws Exception {
    db.close();
  }

  @Override
  public Map<Long, String> getReverseCache() {
    return idCache.getReverseCache();
  }

}
