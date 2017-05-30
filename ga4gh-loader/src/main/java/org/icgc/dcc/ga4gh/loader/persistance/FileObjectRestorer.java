package org.icgc.dcc.ga4gh.loader.persistance;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;

import static lombok.AccessLevel.PRIVATE;

@RequiredArgsConstructor(access =  PRIVATE)
public class FileObjectRestorer<T extends Serializable> implements ObjectRestorer<Path, T> {

  @NonNull
  @Getter
  private final Path persistedPath;

  @Override @SuppressWarnings("unchecked")
  public T restore() throws IOException, ClassNotFoundException {
    if (isPersisted()){
      return (T) ObjectPersistance.restore(getPersistedPath());
    } else {
      throw new IllegalStateException(String.format("Cannot restore if persistedFilename [%s] DNE", getPersistedPath().toString()));
    }
  }

  @Override public void store(T t) throws IOException {
    ObjectPersistance.<T>store(t,getPersistedPath());
  }

  @Override public void clean() throws IOException {
    Files.deleteIfExists(getPersistedPath());
  }

  @Override public boolean isPersisted(){
    return Files.exists(getPersistedPath());
  }

  public static <T extends Serializable> FileObjectRestorer<T> newFileObjectRestorer(Path persistedPath){
    return new FileObjectRestorer<T>(persistedPath);
  }

}
