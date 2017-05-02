package org.icgc.dcc.ga4gh.loader2;

import com.google.common.collect.Maps;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.icgc.dcc.ga4gh.common.model.es.EsCallSet;
import org.icgc.dcc.ga4gh.common.model.portal.PortalMetadata;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.id.IdStorage;

import java.util.Map;
import java.util.Optional;

@RequiredArgsConstructor
public class CallSetDao {

  public static CallSetDao createCallSetDao(IdStorage<EsCallSet, Integer> idStorage) {
    return new CallSetDao(createMap(idStorage));
  }

  private static Map<String, EsCallSet> createMap(IdStorage<EsCallSet, Integer> idStorage) {
    val map = Maps.<String, EsCallSet>newHashMap();
    idStorage.getObjects().forEach(x -> map.put(x.getName(), x));
    return map;
  }

  @NonNull private final Map<String, EsCallSet> map;

  public Optional<EsCallSet> find(PortalMetadata portalMetadata) {
    val callSetName = portalMetadata.getSampleId();
    if (map.containsKey(callSetName)) {
      return Optional.of(map.get(callSetName));
    } else {
      return Optional.empty();
    }
  }

}
