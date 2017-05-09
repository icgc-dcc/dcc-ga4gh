package org.icgc.dcc.ga4gh.loader2.utils.idstorage.context.impl;

import com.google.common.collect.Lists;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.val;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.context.IdStorageContext;
import org.jetbrains.annotations.NotNull;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;

import java.io.IOException;
import java.util.List;

@Value
@RequiredArgsConstructor
public class IdStorageContextImpl<ID, K> implements IdStorageContext<ID, K> {

  public static <ID, K> IdStorageContextImpl<ID, K> createIdStorageContext(ID id) {
    return new IdStorageContextImpl<ID, K>(id);
  }

  @NonNull private final ID id;
  private List<K> objects = Lists.newArrayList();

  @Override public void add(K object){
    objects.add(object);
  }

  @Override public void addAll(List<K> objects){
    this.objects.addAll(objects);
  }

  @RequiredArgsConstructor
  public static class IdStorageContextImplSerializer<ID,K> implements Serializer<IdStorageContext<ID,K>> {

    public static <ID, K> IdStorageContextImplSerializer<ID, K> createIdStorageContextSerializer(
        Serializer<ID> idSerializer, Serializer<K> kSerializer) {
      return new IdStorageContextImplSerializer<ID, K>(idSerializer, kSerializer);
    }

    @NonNull private final Serializer<ID> idSerializer;
    @NonNull private final Serializer<K> kSerializer;

    @Override
    public void serialize(@NotNull DataOutput2 dataOutput2, @NotNull IdStorageContext<ID, K> idkIdStorageContext) throws IOException {
      idSerializer.serialize(dataOutput2, idkIdStorageContext.getId());

      val numObjects= idkIdStorageContext.getObjects().size();
      dataOutput2.packInt(numObjects);
      for (val objects : idkIdStorageContext.getObjects()){
        kSerializer.serialize(dataOutput2,objects);
      }
    }

    @Override public IdStorageContext<ID, K> deserialize(@NotNull DataInput2 dataInput2, int i) throws IOException {
      val id = idSerializer.deserialize(dataInput2, i);
      val numObjects = dataInput2.unpackInt();
      val out = IdStorageContextImpl.<ID,K>createIdStorageContext(id);
      for (int j=0; j<numObjects; j++){
        val object = kSerializer.deserialize(dataInput2,i);
        out.add(object);
      }
      return out;
    }
  }

}
