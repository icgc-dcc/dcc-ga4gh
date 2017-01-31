package org.collaboratory.ga4gh.loader;

import static com.google.common.base.Stopwatch.createStarted;
import static org.collaboratory.ga4gh.core.SearchHitConverters.convertHitToInteger;
import static org.collaboratory.ga4gh.core.SearchHitConverters.convertHitToString;
import static org.collaboratory.ga4gh.core.SearchHitConverters.convertHitToStringList;
import static org.collaboratory.ga4gh.loader.Factory.newClient;
import static org.collaboratory.ga4gh.loader.Factory.newDocumentWriter;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.constantScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.hasChildQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

import org.apache.lucene.search.join.ScoreMode;
import org.collaboratory.ga4gh.loader.model.es.EsCall;
import org.collaboratory.ga4gh.loader.model.es.EsVariant;
import org.collaboratory.ga4gh.loader.model.es.EsVariantCallPair;
import org.collaboratory.ga4gh.loader.test.BaseElasticsearchTest;
import org.collaboratory.ga4gh.loader.utils.CounterMonitor;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.search.SearchHit;
import org.junit.Test;

import com.google.common.collect.Lists;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ExperimentTest extends BaseElasticsearchTest {

  private static EsVariant createVariantFromHit(SearchHit hit) {
    val start = convertHitToInteger(hit, "start");
    val end = convertHitToInteger(hit, "end");
    val referenceName = convertHitToString(hit, "reference_name");
    val referenceBases = convertHitToString(hit, "reference_bases");
    val alternateBases = convertHitToStringList(hit, "alternate_bases");
    return EsVariant.builder()
        .start(start)
        .end(end)
        .referenceName(referenceName)
        .referenceBases(referenceBases)
        .alternativeBases(alternateBases)
        .build();
  }

  // public static void main(String[] args) {
  @Test
  public void testLoad() {
    log.info("Static Config:\n{}", Config.toConfigString());
    try (val client = newClient();
        val writer = newDocumentWriter(client)) {
      log.info("yooo");
      val stopWatch = createStarted();

      val query = constantScoreQuery(
          boolQuery()
              .must(matchAllQuery())
              .must(hasChildQuery("call", matchAllQuery(), ScoreMode.None).innerHit(new InnerHitBuilder())));

      val maxIterations = 25;
      val sizes = new int[] { 5000, 3000, 1500, 1000, 500, 100, 10 };
      int count = 0;
      for (val size : sizes) {
        client.prepareClearScroll();
        log.info("Config[{}]: SIZE: {}   MaxIterations: {}", count++, size, maxIterations);
        SearchResponse resp = client.prepareSearch("dcc-variants_test")
            .setTypes("variant")
            .setSize(size)
            .setScroll(TimeValue.timeValueMinutes(3))
            .setQuery(query)
            .get();

        boolean hasHits = resp.getHits().getTotalHits() > 0;
        val pairList = Lists.<EsVariantCallPair> newArrayList();
        val hitProcessorMonitor = CounterMonitor.newMonitor("SearchHitMonitor", 100);
        val scrollMonitor = CounterMonitor.newMonitor("ScrollMonitor", 1);
        val hitAndScrollMonitor = CounterMonitor.newMonitor("HitAndScrollMonitor", 1);

        do {
          hitProcessorMonitor.start();
          for (val hit : resp.getHits()) {
            val pair = EsVariantCallPair.builder()
                .variant(
                    EsVariant.builder()
                        .fromSearchHit(hit)
                        .build());

            for (val innerHit : hit.getInnerHits().get("call")) {
              pair.call(EsCall.builder().fromSearchHit(innerHit).build());
		hitProcessorMonitor.incr();
            }
            pairList.add(pair.build());
          }
          resp = client.prepareSearchScroll(resp.getScrollId())
              .setScroll(TimeValue.timeValueMinutes(3))
              .get();
          hasHits = resp.getHits().getTotalHits() > 0;
          pairList.clear();
        } while (hasHits && scrollMonitor.getCount() < maxIterations);
          hitProcessorMonitor.stop();
        hitProcessorMonitor.displaySummary();
        log.info(
            "Summary: SIZE: {}  MaxIterations: {}   AvgHitProcessorRate: {}  AvgScrollRate: {}  ",
            size,
            maxIterations,
            hitProcessorMonitor.getAvgRate());
      }
    } catch (Exception e) {
      log.error("Exception running: ", e);
    }

  }

}
