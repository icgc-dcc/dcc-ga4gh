package org.icgc.dcc.ga4gh.loader.utils.idstorage.storage;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import org.icgc.dcc.ga4gh.loader.utils.idstorage.storage.impl.DirectMemoryMapStorage;
import org.icgc.dcc.ga4gh.loader.utils.idstorage.storage.impl.DiskMapStorage;
import org.icgc.dcc.ga4gh.loader.utils.idstorage.storage.impl.RamMapStorage;
import org.mapdb.Serializer;

import java.nio.file.Path;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static org.icgc.dcc.ga4gh.loader.utils.CheckPaths.checkFilePath;
import static org.icgc.dcc.ga4gh.loader.utils.idstorage.storage.impl.DiskMapStorage.generateFilepath;
import static org.icgc.dcc.ga4gh.loader.utils.idstorage.storage.impl.DiskMapStorage.newDiskMapStorage;
import static org.icgc.dcc.ga4gh.loader.utils.idstorage.storage.impl.RamMapStorage.newRamMapStorage;

@RequiredArgsConstructor
public class MapStorageFactory<K, V> {

  public static <K, V> MapStorageFactory<K, V> createMapStorageFactory(String name, Serializer<K> keySerializer,
      Serializer<V> valueSerializer,
      Path outputDir, final long allocation) {
    return new MapStorageFactory<K, V>(name, keySerializer, valueSerializer, outputDir, allocation);
  }

  @NonNull  private final String name;
  @NonNull private final Serializer<K> keySerializer;
  @NonNull private final Serializer<V> valueSerializer;
  @NonNull private final Path outputDir;
  private final long allocation;

  public Path getPath(){
    return generateFilepath(name, outputDir);
  }

  @SneakyThrows
  public DirectMemoryMapStorage<K,V> createDirectMemoryMapStorage(boolean persistFile){
    val dms = createDiskMapStorage(persistFile);
    return DirectMemoryMapStorage.createDirectMemoryMapStorage(name, keySerializer,valueSerializer,dms);
  }

  @SneakyThrows
  public DiskMapStorage<K, V> createDiskMapStorage(boolean persistFile){
    return newDiskMapStorage(name, keySerializer, valueSerializer,outputDir,allocation,persistFile);
  }

  public RamMapStorage<K, V> createRamMapStorage(){
    return newRamMapStorage();
  }

  public MapStorage<K,V> createMapStorage(boolean useDisk, boolean persistFile){
    if (persistFile){
      checkFilePath(getPath());
    }
    return useDisk ? createDiskMapStorage(persistFile) : createRamMapStorage();
  }

  public MapStorage<K,V> createNewMapStorage(boolean useDisk){
    return createMapStorage(useDisk, FALSE);
  }

  public MapStorage<K,V> persistMapStorage(){
    return createMapStorage(TRUE, TRUE);
  }



}
