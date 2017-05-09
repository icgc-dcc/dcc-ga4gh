package org.icgc.dcc.ga4gh.loader2.utils.idstorage.storage.impl;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.storage.MapStorage;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

@Slf4j
public class DiskMapStorage<K, V> implements MapStorage<K, V> {

  // Input dep
  private final String name;
  private final Path outputDir;

  // Internal Deps
  private DB db;
  private final boolean persistFile;
  private final Serializer<K> keySerializer;
  private final Serializer<V> idSerializer;
  private final long allocation;
  private Map<K, V> map;

  public static Path generateFilepath(String name, Path outputDir) {
    return outputDir.resolve( name + ".db");
  }

  public static <K, V> DiskMapStorage<K, V> newDiskMapStorage(final String name,
      final Serializer<K> keySerializer,
      final Serializer<V> idSerializer,
      final Path outputDir,
      final long allocation,
      final boolean persistFile) throws IOException {
    return new DiskMapStorage<K, V>(name, keySerializer, idSerializer, outputDir, allocation, persistFile);
  }

  public DiskMapStorage(@NonNull final String name,
      @NonNull final Serializer<K> keySerializer,
      @NonNull final Serializer<V> idSerializer,
      @NonNull final Path outputDir,
      final long allocation,
      final boolean persistFile)
      throws IOException {
    this.name = name;
    this.outputDir = outputDir;
    this.persistFile = persistFile;
    this.keySerializer = keySerializer;
    this.idSerializer = idSerializer;
    this.allocation = allocation;
    val filename = generateFilepath(name, outputDir);
    init(filename);
  }

  private void init(Path filepath) throws IOException {
    if (!persistFile) {
      Files.deleteIfExists(filepath);
    }
    if (!filepath.getParent().toFile().exists()){
      Files.createDirectories(filepath.getParent());
    }

    this.db = createEntityDB(filepath, this.allocation);
    this.map = newEntityMap(db, name, keySerializer, idSerializer);
  }

  private static DB createEntityDB(Path filepath, final long allocation) {
    return DBMaker
        .fileDB(filepath.toFile())
        .concurrencyDisable()
        .fileMmapEnable()
        .closeOnJvmShutdown()
        .allocateIncrement(allocation)
        .allocateStartSize(allocation)
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
    val filepath = generateFilepath(name, outputDir);
    try {
      close();
      init(filepath);
    } catch (IOException e) {
      log.error("Was not able to purge IdDiskCache: filename: {}", filepath.toString());
    }
  }
}
