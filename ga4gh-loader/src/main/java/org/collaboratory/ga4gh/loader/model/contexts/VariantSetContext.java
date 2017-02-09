package org.collaboratory.ga4gh.loader.model.contexts;

import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableSet;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.collaboratory.ga4gh.loader.model.es.EsVariantSet;
import org.collaboratory.ga4gh.loader.model.es.converters.EsVariantSetConverter;
import org.collaboratory.ga4gh.loader.model.es.converters.FileMetaDataContextConverter;
import org.collaboratory.ga4gh.loader.model.es.converters.FileMetaDataConverter;
import org.collaboratory.ga4gh.loader.utils.cache.IdCache;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Value;
import lombok.val;

@Value
public class VariantSetContext implements Iterable<EsVariantSet> {

  private final static EsVariantSetConverter DEFAULT_CONVERTER = new EsVariantSetConverter();

  private final FileMetaDataContext fileMetaDataContext;

  private final IdCache<EsVariantSet, Integer> variantSetIdCache;

  @Getter(AccessLevel.NONE)
  private final FileMetaDataContextConverter<List<EsVariantSet>> fileMetaDataContextConverter;

  private final Set<EsVariantSet> variantSets;

  public VariantSetContext(FileMetaDataContext fileMetaDataContext, IdCache<EsVariantSet, Integer> variantSetIdCache) {
    this(fileMetaDataContext, variantSetIdCache, DEFAULT_CONVERTER, DEFAULT_CONVERTER);
  }

  public VariantSetContext(FileMetaDataContext fileMetaDataContext, IdCache<EsVariantSet, Integer> variantSetIdCache,
      FileMetaDataConverter<EsVariantSet> variantSetFileMetaDataConverter,
      FileMetaDataContextConverter<List<EsVariantSet>> variantSetFileMetaDataContextConverter) {
    this.fileMetaDataContext = fileMetaDataContext;
    this.variantSetIdCache = variantSetIdCache;
    this.fileMetaDataContextConverter = variantSetFileMetaDataContextConverter;
    this.variantSets = buildVariantSets(fileMetaDataContext);
    processVariantIdCache();
  }

  private Set<EsVariantSet> buildVariantSets(FileMetaDataContext context) {
    return fileMetaDataContextConverter.convertFromFileMetaDataContext(context)
        .stream()
        .collect(toImmutableSet());
  }

  public List<Integer> getVariantSetIds(FileMetaDataContext context) {
    return buildVariantSets(context).stream()
        .map(variantSetIdCache::getId)
        .collect(toImmutableList());
  }

  private void processVariantIdCache() {
    for (val variantSet : variantSets) {
      if (!variantSetIdCache.contains(variantSet)) {
        variantSetIdCache.add(variantSet);
      }
    }
  }

  @Override
  public Iterator<EsVariantSet> iterator() {
    return variantSets.iterator();
  }
}
