package org.icgc.dcc.ga4gh.loader2.utils.idstorage.id.impl;

import lombok.NonNull;
import lombok.val;
import org.icgc.dcc.ga4gh.common.model.es.EsCall;
import org.icgc.dcc.ga4gh.common.model.es.EsVariant;
import org.icgc.dcc.ga4gh.common.model.es.EsVariantCallPair2;
import org.icgc.dcc.ga4gh.loader2.utils.Counter2;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.context.IdStorageContext;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.context.impl.IdStorageContextImpl;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.id.IdStorage;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.storage.MapStorage;

import java.util.Map;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class VariantIdStorage<N> implements IdStorage<EsVariantCallPair2, IdStorageContext<N, EsCall>> {

  private static final long SINGLE_INCREMENT_AMOUNT = 1L;

  public static <N> VariantIdStorage<N> createVariantIdStorage(Counter2<N> counter,
      MapStorage<EsVariant, IdStorageContext<N, EsCall>> mapStorage) {
    return new VariantIdStorage<N>(counter, mapStorage);
  }


  @NonNull private final Counter2<N> counter;
  @NonNull private final MapStorage<EsVariant, IdStorageContext<N, EsCall>> mapStorage;
  private Map<EsVariant, IdStorageContext<N, EsCall>> map;

  private VariantIdStorage(Counter2<N> counter,
      MapStorage<EsVariant, IdStorageContext<N, EsCall>> mapStorage) {
    this.counter = counter;
    this.mapStorage = mapStorage;
    this.map = mapStorage.getMap();

    this.checkIdLowerBound();
    this.checkIdUpperBound();
  }

  @Override public void purge() {
    mapStorage.purge();
  }

  private void checkIdLowerBound() {
    val count = counter.getCount();
    val min = counter.getMin();
    checkState(counter.countGTE(min), "The id %d must be >= %d", count, min);
  }

  private void checkIdUpperBound() {
    val count = counter.getCount();
    val max = counter.getMax();
    checkState(counter.countLT(max), "The id %d must be >= %d", count, max);
  }

  @Override public void add(EsVariantCallPair2 esVariantCallPair) {
    val esCalls = esVariantCallPair.getCalls();
    val esVariant = esVariantCallPair.getVariant();
    if (!containsObject(esVariantCallPair)){
      counter.incr();
      val ctx = IdStorageContextImpl.<N, EsCall>createIdStorageContext(counter.getCount());
      ctx.addAll(esCalls);
      mapStorage.getMap().put(esVariant, ctx);
    } else {
      val ctx = mapStorage.getMap().get(esVariant);
      ctx.addAll(esCalls);
    }
  }

  @Override public boolean containsObject(EsVariantCallPair2 esVariantCallPair2) {
    return map.containsKey(esVariantCallPair2.getVariant());
  }

  @Override public IdStorageContext<N, EsCall> getId(EsVariantCallPair2 esVariantCallPair2) {
    val variant = esVariantCallPair2.getVariant();
    checkArgument(containsObject(esVariantCallPair2), "The following key doesnt not exist in the idstorage: \n%s", esVariantCallPair2.getVariant());
    return map.get(variant);
  }


  @Override public String getIdAsString(EsVariantCallPair2 esVariantCallPair2) {
    return getId(esVariantCallPair2).getId().toString();
  }

  @Override
  public Stream<Map.Entry<EsVariantCallPair2, IdStorageContext<N, EsCall>>> streamEntries(){
    return map.entrySet().stream().map(this::convertEntry);
  }

  private Map.Entry<EsVariantCallPair2, IdStorageContext<N, EsCall>> convertEntry(Map.Entry<EsVariant, IdStorageContext<N, EsCall>> entry){
    return new Map.Entry<EsVariantCallPair2, IdStorageContext<N, EsCall>>(){

      @Override public EsVariantCallPair2 getKey() {
        return EsVariantCallPair2.createEsVariantCallPair2(entry.getKey(), entry.getValue().getObjects());
      }

      @Override public IdStorageContext<N, EsCall> getValue() {
        return entry.getValue();
      }

      @Override public IdStorageContext<N, EsCall> setValue(IdStorageContext<N, EsCall> value) {
        throw new IllegalStateException("Should not be using this");
      }

    };

  }
}
