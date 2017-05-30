package org.icgc.dcc.ga4gh.loader;

import com.google.common.collect.Sets;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.icgc.dcc.ga4gh.common.model.es.EsCallSet;
import org.icgc.dcc.ga4gh.loader.utils.idstorage.id.IdStorage;
import org.icgc.dcc.ga4gh.loader.utils.idstorage.storage.MapStorage;
import org.icgc.dcc.ga4gh.loader.utils.idstorage.storage.impl.RamMapStorage;

import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static org.icgc.dcc.common.core.util.stream.Streams.stream;
import static org.icgc.dcc.ga4gh.common.model.es.EsCallSet.createEsCallSet;
import static org.icgc.dcc.ga4gh.loader.utils.idstorage.id.impl.IntegerIdStorage.createIntegerIdStorage;

@RequiredArgsConstructor
public class CallSetAccumulator {

  private final Map<String,Set<Integer>> variantSetIdsMap;
  private final Map<String, Integer> callSetIdMap;

  private int idCount = 0;

  public void addVariantSetId(String callSetName, int variantSetId){
    if (!variantSetIdsMap.containsKey(callSetName)){
      variantSetIdsMap.put(callSetName, Sets.newHashSet(variantSetId));
      callSetIdMap.put(callSetName, ++idCount);
    } else {
      val idSet = variantSetIdsMap.get(callSetName);
      idSet.add(variantSetId);
    }
  }

  public void addVariantSetIds(String callSetName, Iterable<Integer> variantSetIds){
    stream(variantSetIds).forEach(x -> addVariantSetId(callSetName, x));
  }

  public int getId(String callSetName){
    checkArgument(callSetIdMap.containsKey(callSetName), "The callSetName [%s] DNE", callSetName);
    return callSetIdMap.get(callSetName);
  }

  public EsCallSet buildEsCallSet(String callSetName){
    return createEsCallSet(callSetName,callSetName,variantSetIdsMap.get(callSetName));
  }

  public IdStorage<EsCallSet, Integer> getIdStorage(){
    return createIntegerIdStorage(getMapStorage(), idCount+1);
  }

  public MapStorage<EsCallSet, Integer> getMapStorage(){
    val mapStorage = RamMapStorage.<EsCallSet, Integer>newRamMapStorage();
    val map = mapStorage.getMap();
    for (val callSetName : variantSetIdsMap.keySet()){
      val esCallSet = buildEsCallSet(callSetName);
      val callSetId = callSetIdMap.get(callSetName);
      map.put(esCallSet, callSetId);
    }
    return mapStorage;
  }

  public Stream<EsCallSet> streamCallSets(){
    return variantSetIdsMap.keySet().stream()
        .map(this::buildEsCallSet);
  }

  public static CallSetAccumulator createCallSetAccumulator(Map<String, Set<Integer>> variantSetIdsMap,
      Map<String, Integer> callSetIdMap) {
    return new CallSetAccumulator(variantSetIdsMap, callSetIdMap);
  }


}
