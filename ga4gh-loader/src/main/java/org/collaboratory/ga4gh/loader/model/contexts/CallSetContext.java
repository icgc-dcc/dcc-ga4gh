package org.collaboratory.ga4gh.loader.model.contexts;

import java.util.Iterator;
import java.util.Set;

import org.collaboratory.ga4gh.loader.model.es.EsCallSet;
import org.collaboratory.ga4gh.loader.model.es.EsVariantSet;
import org.collaboratory.ga4gh.loader.model.es.converters.EsCallSetConverter;
import org.collaboratory.ga4gh.loader.utils.cache.IdCache;

import lombok.Getter;
import lombok.NonNull;
import lombok.val;

/*
 * Due to the nature of the model, VariantSetContext and CallSetContext are VERY coupled
 */
public class CallSetContext implements Iterable<EsCallSet> {

  private static final EsCallSetConverter DEFAULT_CONVERTER = new EsCallSetConverter();

  private final FileMetaDataContext fileMetaDataContext;

  private final IdCache<EsVariantSet, Integer> variantSetIdCache;
  private final IdCache<EsCallSet, Integer> callSetIdCache;

  private final EsCallSetConverter converter;

  @Getter
  private final Set<EsCallSet> callSets;

  public CallSetContext(FileMetaDataContext fileMetaDataContext,
      IdCache<EsVariantSet, Integer> variantSetIdCache,
      IdCache<EsCallSet, Integer> callSetIdCache) {
    this(fileMetaDataContext, variantSetIdCache, callSetIdCache, DEFAULT_CONVERTER);
  }

  public CallSetContext(@NonNull FileMetaDataContext fileMetaDataContext,
      @NonNull IdCache<EsVariantSet, Integer> variantSetIdCache,
      @NonNull IdCache<EsCallSet, Integer> callSetIdCache,
      @NonNull EsCallSetConverter converter) {
    this.fileMetaDataContext = fileMetaDataContext;
    this.variantSetIdCache = variantSetIdCache;
    this.callSetIdCache = callSetIdCache;
    this.converter = converter;
    this.callSets = createCallSets();
    populateCallSetIdCache();
  }

  private void populateCallSetIdCache() {
    for (val callSet : callSets) {
      if (!callSetIdCache.contains(callSet)) {
        callSetIdCache.add(callSet);
      }
    }
  }

  private Set<EsCallSet> createCallSets() {
    return converter.convertFromFileMetaData(fileMetaDataContext, variantSetIdCache);
  }

  @Override
  public Iterator<EsCallSet> iterator() {
    return callSets.iterator();
  }

}
