package org.icgc.dcc.ga4gh.loader.utils.idstorage.id.impl;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.ga4gh.common.model.es.EsCall;
import org.icgc.dcc.ga4gh.common.model.es.EsVariant;
import org.icgc.dcc.ga4gh.common.model.es.EsVariantCallPair;
import org.icgc.dcc.ga4gh.loader.utils.counting.Counter;
import org.icgc.dcc.ga4gh.loader.utils.idstorage.context.IdStorageContext;
import org.icgc.dcc.ga4gh.loader.utils.idstorage.context.impl.IdStorageContextImpl;
import org.icgc.dcc.ga4gh.loader.utils.idstorage.id.IdStorage;
import org.icgc.dcc.ga4gh.loader.utils.idstorage.storage.MapStorage;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;

@Slf4j
public class VariantIdStorage<N> implements IdStorage<EsVariantCallPair, IdStorageContext<N, EsCall>>, MapStorage<EsVariant, IdStorageContext<N, EsCall>> {

  private static final long SINGLE_INCREMENT_AMOUNT = 1L;

  public static <N> VariantIdStorage<N> createVariantIdStorage(Counter<Long> counter,
      MapStorage<EsVariant, IdStorageContext<N, EsCall>> mapStorage) {
    return new VariantIdStorage<N>(counter, mapStorage);
  }


  @NonNull private final Counter<Long> counter;
  @NonNull private final MapStorage<EsVariant, IdStorageContext<N, EsCall>> mapStorage;
  private Map<EsVariant, IdStorageContext<N, EsCall>> map;

  private VariantIdStorage(Counter<Long> counter,
      MapStorage<EsVariant, IdStorageContext<N, EsCall>> mapStorage) {
    this.counter = counter;
    this.mapStorage = mapStorage;
    this.map = mapStorage.getMap();

  }

  @Override public void purge() {
    mapStorage.purge();
  }

  @Override public void add(EsVariantCallPair esVariantCallPair) {
    val esCalls = esVariantCallPair.getCalls();
    val esVariant = esVariantCallPair.getVariant();
    if (!containsObject(esVariantCallPair)){
      counter.incr();
      val ctx = IdStorageContextImpl.<N, EsCall>createIdStorageContext((N)counter.getCount());
      ctx.addAll(esCalls);
      mapStorage.getMap().put(esVariant, ctx);
    } else {
      val ctx = mapStorage.getMap().get(esVariant);
      ctx.addAll(esCalls);
    }
  }

  @Override public boolean containsObject(EsVariantCallPair esVariantCallPair) {
    return map.containsKey(esVariantCallPair.getVariant());
  }

  @Override public IdStorageContext<N, EsCall> getId(EsVariantCallPair esVariantCallPair) {
    val variant = esVariantCallPair.getVariant();
    checkArgument(containsObject(esVariantCallPair), "The following key doesnt not exist in the idstorage: \n%s", esVariantCallPair
        .getVariant());
    return map.get(variant);
  }


  @Override public String getIdAsString(EsVariantCallPair esVariantCallPair) {
    return getId(esVariantCallPair).getId().toString();
  }

  @Override
  public Stream<Map.Entry<EsVariantCallPair, IdStorageContext<N, EsCall>>> streamEntries(){
    return map.entrySet().stream().map(this::convertEntry);
  }

  private Map.Entry<EsVariantCallPair, IdStorageContext<N, EsCall>> convertEntry(Map.Entry<EsVariant, IdStorageContext<N, EsCall>> entry){
    return new Map.Entry<EsVariantCallPair, IdStorageContext<N, EsCall>>(){

      @Override public EsVariantCallPair getKey() {
        return EsVariantCallPair.createEsVariantCallPair2(entry.getKey(), entry.getValue().getObjects());
      }

      @Override public IdStorageContext<N, EsCall> getValue() {
        return entry.getValue();
      }

      @Override public IdStorageContext<N, EsCall> setValue(IdStorageContext<N, EsCall> value) {
        throw new IllegalStateException("Should not be using this");
      }

    };

  }

  @Override public void close() throws IOException {
    if (this.mapStorage != null){
      try {
        this.mapStorage.close();
      } catch (Throwable t){
        log.error("Could not close MapStorage [{}]", this.getClass().getName());
      }
    }
  }

  @Override public Map<EsVariant, IdStorageContext<N, EsCall>> getMap() {
    return this.mapStorage.getMap();
  }

}
