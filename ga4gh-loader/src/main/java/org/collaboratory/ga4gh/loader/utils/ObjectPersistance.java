package org.collaboratory.ga4gh.loader.utils;

import static lombok.AccessLevel.PRIVATE;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import lombok.NoArgsConstructor;

@NoArgsConstructor(access = PRIVATE)
public class ObjectPersistance {

  public static void store(final Object object, final String filename) throws IOException {
    FileOutputStream fout = null;
    ObjectOutputStream oos = null;
    fout = new FileOutputStream(filename);
    oos = new ObjectOutputStream(fout);
    oos.writeObject(object);
    if (fout != null) {
      fout.close();
    }
    if (oos != null) {
      oos.close();
    }
  }

  public static Object restore(final String filename) throws ClassNotFoundException, IOException {
    FileInputStream fin = null;
    ObjectInputStream ois = null;
    fin = new FileInputStream(filename);
    ois = new ObjectInputStream(fin);
    Object object = ois.readObject();
    if (fin != null) {
      fin.close();
    }
    if (ois != null) {
      ois.close();
    }
    return object;
  }

}
