package org.icgc.dcc.ga4gh.common;

import lombok.Cleanup;
import lombok.NoArgsConstructor;
import lombok.val;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static lombok.AccessLevel.PRIVATE;

@NoArgsConstructor(access = PRIVATE)
public class ObjectPersistance {

  public static void store(final Object object, final String filename) throws IOException {
    @Cleanup
    val fout = new FileOutputStream(filename);
    val oos = new ObjectOutputStream(fout);
    oos.writeObject(object);
  }

  public static Object restore(final String filename) throws ClassNotFoundException, IOException {
    @Cleanup
    val fin = new FileInputStream(filename);
    val ois = new ObjectInputStream(fin);
    return ois.readObject();
  }

}
