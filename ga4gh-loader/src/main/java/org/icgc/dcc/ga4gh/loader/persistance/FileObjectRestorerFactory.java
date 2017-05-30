package org.icgc.dcc.ga4gh.loader.persistance;

import com.google.common.collect.Sets;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.function.Supplier;

import static org.icgc.dcc.common.core.util.Joiners.DOT;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableSet;
import static org.icgc.dcc.ga4gh.loader.persistance.FileObjectRestorer.newFileObjectRestorer;

public class FileObjectRestorerFactory {

  private static final String DEFAULT_EXT = "dat";

  @NonNull private final Path outputDir;

  private Set<FileObjectRestorer<? extends Object>> fileObjectRestorers;

  @SneakyThrows
  public FileObjectRestorerFactory(Path outputDir){
    this.outputDir = outputDir;
    initDir(outputDir);
    fileObjectRestorers = Sets.newHashSet();
  }

  public <T extends Serializable> ObjectRestorer<Path, T> createFileRestorer(String filename){
    val path = outputDir.resolve(filename);
    final FileObjectRestorer<T> fileObjectRestorer =  newFileObjectRestorer(path);
    fileObjectRestorers.add(fileObjectRestorer);
    return fileObjectRestorer;
  }

  public <T extends Serializable> ObjectRestorer<Path, T> createFileRestorerByName(String objectName){
    return createFileRestorer(DOT.join(objectName,DEFAULT_EXT));
  }

  public Set<Path> getPaths(){
    return fileObjectRestorers.stream()
        .map(FileObjectRestorer::getPersistedPath)
        .collect(toImmutableSet());
  }

  public void clean(String persistanceName ){
    fileObjectRestorers.stream()
        .filter(x -> getObjectName(x.getPersistedPath()).equals(persistanceName))
        .forEach(FileObjectRestorerFactory::cleanFileRestorer);
  }

  public void cleanAll(){
    fileObjectRestorers.forEach(FileObjectRestorerFactory::cleanFileRestorer);
  }


  public <T extends Serializable> T persistObject(String persistanceName, Supplier<T> objectCreationSupplier) throws IOException, ClassNotFoundException {
    val restorer = this.<T>createFileRestorerByName(persistanceName);
    T object = null;
    if (restorer.isPersisted()){
      object = restorer.restore();
    } else {
      object = objectCreationSupplier.get();
      restorer.store(object);
    }
    return object;
  }

  @SneakyThrows
  private static <A, B extends Serializable> void cleanFileRestorer(ObjectRestorer<A,B> fr){
    fr.clean();
  }

  private static void initDir(Path outputDir) throws IOException {
    if (!outputDir.toFile().exists()){
      Files.createDirectories(outputDir);
    }
  }

  private static String getObjectName(Path filepath){
    return filepath.getFileName().toString().replaceAll("."+DEFAULT_EXT+"$", "");
  }

  public static final FileObjectRestorerFactory createFileObjectRestorerFactory(Path outputDir){
    return new FileObjectRestorerFactory(outputDir);
  }

  public static final FileObjectRestorerFactory createFileObjectRestorerFactory(String outputDirname){
    return createFileObjectRestorerFactory(Paths.get(outputDirname));
  }

}
