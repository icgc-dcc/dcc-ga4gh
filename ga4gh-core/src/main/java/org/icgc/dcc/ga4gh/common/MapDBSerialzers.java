package org.icgc.dcc.ga4gh.common;

import com.google.common.collect.Lists;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;

import java.util.List;

import static lombok.AccessLevel.PRIVATE;

@NoArgsConstructor(access = PRIVATE)
public class MapDBSerialzers {

  @SneakyThrows
  public static <T> void serializeList(DataOutput2 out, Serializer<T> serializer, List<T> objects){
    val n = objects.size();
    out.packInt(n);
    for (val object : objects){
      serializer.serialize(out, object);
    }
  }

  @SneakyThrows
  public static <T> List<T> deserializeList(DataInput2 in, int available, Serializer<T> serializer){
    val list = Lists.<T>newArrayList();
    val size = in.unpackInt();
    for (int i=0;i<size;i++){
      val object = serializer.deserialize(in,available);
      list.add(object);
    }
    return list;
  }

}
