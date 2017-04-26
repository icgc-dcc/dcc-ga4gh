package org.icgc.dcc.ga4gh.loader.indexing;

import com.google.common.collect.Lists;
import lombok.Builder;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.experimental.NonFinal;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.lucene.search.join.ScoreMode;
import org.icgc.dcc.ga4gh.common.model.converters.JsonObjectNodeConverter;
import org.icgc.dcc.ga4gh.common.model.converters.SearchHitConverter;
import org.icgc.dcc.ga4gh.common.model.es.EsVariantCallPair;
import org.icgc.dcc.ga4gh.loader.utils.CounterMonitor;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.ConstantScoreQueryBuilder;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.icgc.dcc.dcc.common.es.core.DocumentWriter;
import org.icgc.dcc.dcc.common.es.impl.IndexDocumentType;
import org.icgc.dcc.dcc.common.es.model.IndexDocument;

import static org.icgc.dcc.ga4gh.common.TypeNames.VARIANT_NESTED;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.constantScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.hasChildQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

@Builder
@Slf4j
public class ParentChild2NestedIndexConverter {

  public static final String VARIANT_NESTED_TYPE_NAME = "variant_nested";

  private static final int DEFAULT_SCROLL_TIMEOUT_MINUTES = 10;

  @NonNull
  private final Client client;

  @NonNull
  private final DocumentWriter writer;

  @NonNull
  private final String sourceIndexName;

  @NonNull
  private final String targetIndexName;

  @NonNull
  private final String parentTypeName;

  @NonNull
  private final String targetTypeName;

  @NonNull
  private final String childTypeName;

  private final int scrollSize;

  @NonNull
  private final SearchHitConverter<EsVariantCallPair> searchHitConverter;

  @NonNull
  private final JsonObjectNodeConverter<EsVariantCallPair> nestedJsonObjectNodeConverter;

  private final CounterMonitor variantMonitor = CounterMonitor.newMonitor("VariantMonitor", 2000);

  @NonFinal
  private long variantId = 1L;

  @SneakyThrows
  private void prepareIndex() {
    val indexCreatorContext = IndexCreatorContext.builder()
        .client(client)
        .indexingEnabled(true)
        .indexSettingsFilename(Indexer.INDEX_SETTINGS_JSON_FILENAME)
        .mappingFilenameExtension(Indexer.DEFAULT_MAPPING_JSON_EXTENSION)
        .indexName(targetIndexName)
        .mappingDirname(Indexer.DEFAULT_MAPPINGS_DIRNAME)
        .typeName(targetTypeName)
        .build();

    val indexCreator = new IndexCreator(indexCreatorContext);
    indexCreator.execute();
  }

  public void execute() {
    prepareIndex();
    variantMonitor.start();
    val initialSearchResponse = doInitialSearch();
    variantMonitor.stop();
    boolean hasHits = initialSearchResponse.getHits().getTotalHits() > 0;
    val pairList = Lists.<EsVariantCallPair> newArrayList();
    SearchResponse resp = initialSearchResponse;
    SearchResponse prevResp;

    int count = 0;

    variantMonitor.start();
    while (hasHits) {
      prevResp = resp;
      for (val hit : resp.getHits()) {
        val pair = searchHitConverter.convertFromSearchHit(hit);
        pairList.add(pair);
      }
      for (val pair : pairList) {
        indexPair(pair);
      }
      variantMonitor.stop();
      variantMonitor.displaySummary();
      variantMonitor.start();

      resp = client.prepareSearchScroll(resp.getScrollId())
          .setScroll(TimeValue.timeValueMinutes(DEFAULT_SCROLL_TIMEOUT_MINUTES))
          .get();
      hasHits = resp.getHits().getTotalHits() > 0;
      pairList.clear();
      log.info(
          "Scroll Summary[{}]: SIZE: {}  AvgCallProcessorRate: {}  TotalCallsIndexed: {} ",
          count++,
          prevResp.getHits().getTotalHits(),
          variantMonitor.getAvgRate(),
          variantMonitor.getCount());
    }

  }

  private static class VariantNestedDocumentType implements IndexDocumentType {

    @Override
    public String getIndexType() {
      return VARIANT_NESTED;
    }

  }

  @SneakyThrows
  private void indexPair(EsVariantCallPair pair) {
    writer.write(new IndexDocument(getNextVariantId(), nestedJsonObjectNodeConverter.convertToObjectNode(pair),
        new VariantNestedDocumentType()));
    variantMonitor.incr();
  }

  private String getNextVariantId() {
    return Long.toString(variantId++);
  }

  private ConstantScoreQueryBuilder createScrollQuery() {
    return constantScoreQuery(
        boolQuery()
            .must(matchAllQuery())
            .must(hasChildQuery(childTypeName, matchAllQuery(), ScoreMode.None).innerHit(new InnerHitBuilder())));
  }

  private SearchResponse doInitialSearch() {
    client.prepareClearScroll();
    return client.prepareSearch(sourceIndexName)
        .setTypes(parentTypeName)
        .setSize(scrollSize)
        .setScroll(TimeValue.timeValueMinutes(DEFAULT_SCROLL_TIMEOUT_MINUTES))
        .setQuery(createScrollQuery())
        .get();
  }

  // Injection
  // source index name
  // target index name (in IndexCreatorContext)
  // target index mapping (so variant mapping with nested structure) (in IndexCreatorContext). For now
  // ...persistObject manual mapping.json file, but later might want some automatic solution

  // parent type name
  // scroll timeout

  // child type name
  // searchHitConverter<T> for response

  // Instructions:
  // persistObject target index
  // reindex everything but variants and calls (get all types, and exclude parents and child)
  // persistObject query,
  // scroll through
  // Convert searchhit to esModel

  // index model as Nested

}
