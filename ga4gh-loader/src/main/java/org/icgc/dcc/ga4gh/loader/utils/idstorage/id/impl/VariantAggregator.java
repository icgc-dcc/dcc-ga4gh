package org.icgc.dcc.ga4gh.loader.utils.idstorage.id.impl;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.ga4gh.common.model.es.EsConsensusCall;
import org.icgc.dcc.ga4gh.common.model.es.EsVariant;
import org.icgc.dcc.ga4gh.loader.utils.Purgeable;
import org.icgc.dcc.ga4gh.loader.utils.idstorage.storage.MapStorage;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static com.google.common.collect.Lists.newArrayList;
import static org.icgc.dcc.ga4gh.common.model.es.EsVariantCallPair.createEsVariantCallPair;

@Slf4j
public class VariantAggregator implements Purgeable, Closeable {

  public static VariantAggregator createVariantAggregator(MapStorage<EsVariant, List<EsConsensusCall>> mapStorage) {
    return new VariantAggregator(mapStorage);
  }

  @NonNull private final MapStorage<EsVariant, List< EsConsensusCall>> mapStorage;
  private Map<EsVariant, List<EsConsensusCall>> map;
  private long count = 0;

  public VariantAggregator( MapStorage<EsVariant, List<EsConsensusCall>> mapStorage) {
    this.mapStorage = mapStorage;
    this.map = mapStorage.getMap();
  }

  @Override public void purge() {
    mapStorage.purge();
  }

  public void add(EsVariant esVariant, List<EsConsensusCall> esCalls) {
    esCalls.forEach(x -> add(esVariant, x  ));
  }

  public void add(EsVariant esVariant, EsConsensusCall esCall) {
    if (!map.containsKey(esVariant)){
      val callList = newArrayList(esCall);
      map.put(esVariant, callList);
    } else {
      val callList = map.get(esVariant);
      callList.add(esCall);
      map.put(esVariant, callList); //rtisma refer to JIRA ticket [https://jira.oicr.on.ca/browse/DCC-5587] -- [GA4GH] DiskMapStorage disk commit issue
    }
  }

  private void resetCount(){
    this.count = 0;
  }

  private long incrCount(){
    return count++;
  }

  private VariantIdContext<Long> procEntry(Map.Entry<EsVariant, List<EsConsensusCall>> entry){
    val esVariantCallPair = createEsVariantCallPair(entry.getKey(), entry.getValue());
    return VariantIdContext.<Long>createVariantIdContext(incrCount(),esVariantCallPair);
  }


  public Stream<VariantIdContext<Long>> streamVariantIdContext() {
    resetCount();
    return map.entrySet().stream()
        .map(this::procEntry);
  }

  @Override
  public void close() throws IOException {
    if (this.mapStorage != null){
      try {
        this.mapStorage.close();
      } catch (Throwable t){
        log.error("Could not close MapStorage [{}]", this.getClass().getName());
      }
    }
  }

}
