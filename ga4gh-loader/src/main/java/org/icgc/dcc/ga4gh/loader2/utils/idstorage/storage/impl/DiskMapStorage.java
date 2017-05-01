package org.icgc.dcc.ga4gh.loader2.utils.idstorage.storage.impl;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.common.core.util.Joiners;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.storage.MapStorage;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

@Slf4j
public class DiskMapStorage<K, V> implements MapStorage<K, V> {

  // Input dep
  private final String name;
  private final String outputDirname;

  // Internal Deps
  private DB db;
  private final boolean persistFile;
  private final Serializer<K> keySerializer;
  private final Serializer<V> idSerializer;
  private Map<K, V> map;

  private static String generateFilename(String name, String outputDirname) {
    return Joiners.PATH.join(outputDirname, name + ".db");
  }

  public static <K, V> DiskMapStorage<K, V> newDiskMapStorage(final String name,
      final Serializer<K> keySerializer,
      final Serializer<V> idSerializer,
      final String outputDirname,
      final boolean persistFile) throws IOException {
    return new DiskMapStorage<K, V>(name, keySerializer, idSerializer, outputDirname, persistFile);
  }

  public DiskMapStorage(@NonNull final String name,
      @NonNull final Serializer<K> keySerializer,
      @NonNull final Serializer<V> idSerializer,
      @NonNull final String outputDirname,
      final boolean persistFile)
      throws IOException {
    this.name = name;
    this.outputDirname = outputDirname;
    this.persistFile = persistFile;
    this.keySerializer = keySerializer;
    this.idSerializer = idSerializer;
    val filename = generateFilename(name, outputDirname);
    init(filename);
  }

  private void init(String filename) throws IOException {
    if (!persistFile) {
      Files.deleteIfExists(Paths.get(filename));
    }
    this.db = createEntityDB(filename);
    this.map = newEntityMap(db, name, keySerializer, idSerializer);
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

  private static <K, V> Map<K, V> newEntityMap(DB db, String name, Serializer<K> keySerializer,
      Serializer<V> idSerializer) {
    return db
        .hashMap(name, keySerializer, idSerializer)
        .createOrOpen();
  }

  @Override
  public Map<K, V> getMap() {
    return map;
  }

  @Override
  public void close() throws IOException {
    db.close();
  }

  @Override
  public void purge() {
    val filename = generateFilename(name, outputDirname);
    try {
      close();
      init(filename);
    } catch (IOException e) {
      log.error("Was not able to purge IdDiskCache: filename: {}", filename);
    }
  }
}
