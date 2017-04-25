package org.icgc.dcc.ga4gh.loader2.persistance;

import java.io.IOException;
import java.io.Serializable;

public interface FileRestorer<P, T extends Serializable> {

  @SuppressWarnings("unchecked") T restore() throws IOException, ClassNotFoundException;

  void store(T t) throws IOException;

  void clean() throws IOException;

  boolean isPersisted();

  P getPersistedPath();

}
