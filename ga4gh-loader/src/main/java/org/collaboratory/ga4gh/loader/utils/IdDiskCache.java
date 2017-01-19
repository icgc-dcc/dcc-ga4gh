package org.collaboratory.ga4gh.loader.utils;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

import org.icgc.dcc.common.core.util.Joiners;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class IdDiskCache implements IdCache<String>, Closeable {

  // Input dep
  private final String name;
  private final String outputDirname;

  // Internal Deps
  private DB db;
  private IdCache<String> idCache;
  private final long initId;
  private final boolean persistFile;

  public IdDiskCache(String name, String outputDirname, boolean persistFile) throws IOException {
    this(name, outputDirname, 0L, persistFile);
  }

  public IdDiskCache(String name, String outputDirname) throws IOException {
    this(name, outputDirname, 0L, false);
  }

  private static String generateFilename(String name, String outputDirname) {
    return Joiners.PATH.join(outputDirname, name + ".db");
  }

  public IdDiskCache(String name, String outputDirname, Long initId) throws IOException {
    this(name, outputDirname, initId, false);
  }

  public IdDiskCache(String name, String outputDirname, Long initId, boolean persistFile) throws IOException {
    this.name = name;
    this.outputDirname = outputDirname;
    this.initId = initId;
    this.persistFile = persistFile;
    val filename = generateFilename(name, outputDirname);
    init(filename);
  }

  private void init(String filename) throws IOException {
    if (!persistFile) {
      Files.deleteIfExists(Paths.get(filename));
    }
    this.db = createEntityDB(filename);
    val map = newEntityMap(db, name);
    this.idCache = IdRamCache.newIdCache(map, initId);
  }

  private static DB createEntityDB(String filename) {
    val file = new File(filename); // TODO: add better file management here
    return DBMaker
        .fileDB(file)
        .concurrencyDisable()
        .fileMmapEnable()
        .closeOnJvmShutdown()
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
  public void close() throws IOException {
    db.close();
  }

  @Override
  public Map<Long, String> getReverseCache() {
    return idCache.getReverseCache();
  }

  @Override
  public void purge() {
    val filename = generateFilename(name, outputDirname);
    try {
      db.close();
      init(filename);
    } catch (IOException e) {
      log.error("Was not able to purge IdDiskCache: filename: {}", filename);
    }
  }

}
