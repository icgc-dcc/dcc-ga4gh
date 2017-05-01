package org.icgc.dcc.ga4gh.loader2.utils.idstorage.storage;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.storage.impl.DiskMapStorage;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.storage.impl.RamMapStorage;
import org.mapdb.Serializer;

import java.nio.file.Path;

import static org.icgc.dcc.ga4gh.loader2.utils.idstorage.storage.impl.DiskMapStorage.newDiskMapStorage;
import static org.icgc.dcc.ga4gh.loader2.utils.idstorage.storage.impl.RamMapStorage.newRamMapStorage;

@RequiredArgsConstructor
public class MapStorageFactory<K, V> {

  public static <K, V> MapStorageFactory<K, V> createMapStorageFactory(String name, Serializer<K> keySerializer,
      Serializer<V> valueSerializer,
      Path outputDir, boolean persistFile) {
    return new MapStorageFactory<K, V>(name, keySerializer, valueSerializer, outputDir, persistFile);
  }

  @NonNull  private final String name;
  @NonNull private final Serializer<K> keySerializer;
  @NonNull private final Serializer<V> valueSerializer;
  @NonNull private final Path outputDir;
  private final boolean persistFile;

  @SneakyThrows
  public DiskMapStorage<K, V> createDiskMapStorage(){
    return newDiskMapStorage(name, keySerializer, valueSerializer,outputDir.toString(),persistFile);
  }

  public RamMapStorage<K, V> createRamMapStorage(){
    return newRamMapStorage();
  }

  public MapStorage<K,V> createMapStorage(boolean useDisk){
    return useDisk ? createDiskMapStorage() : createRamMapStorage();
  }

}
