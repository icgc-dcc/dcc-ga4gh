package org.collaboratory.ga4gh.loader;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.lucene.search.join.ScoreMode;
import org.collaboratory.ga4gh.core.model.converters.EsCallConverter;
import org.collaboratory.ga4gh.core.model.converters.EsCallSetConverter;
import org.collaboratory.ga4gh.core.model.converters.EsVariantConverter;
import org.collaboratory.ga4gh.core.model.converters.EsVariantSetConverter;
import org.collaboratory.ga4gh.core.model.es.EsVariantCallPair;
import org.collaboratory.ga4gh.loader.test.BaseElasticsearchTest;
import org.collaboratory.ga4gh.loader.utils.CounterMonitor;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.junit.Ignore;
import org.junit.Test;

import static com.google.common.base.Stopwatch.createStarted;
import static org.collaboratory.ga4gh.loader.factory.MainFactory.newClient;
import static org.collaboratory.ga4gh.loader.factory.MainFactory.newParentChildDocumentWriter;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.constantScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.hasChildQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

@Slf4j
public class ExperimentTest extends BaseElasticsearchTest {

  private static final EsVariantConverter VARIANT_CONVERTER = new EsVariantConverter();
  private static final EsVariantSetConverter VARIANT_SET_CONVERTER = new EsVariantSetConverter();
  private static final EsCallSetConverter CALL_SET_CONVERTER = new EsCallSetConverter();
  private static final EsCallConverter CALL_CONVERTER = new EsCallConverter();

  // public static void main(String[] args) {
  @Test
  @Ignore
  public void testLoad() {
    log.info("Static Config:\n{}", Config.toConfigString());
    try (val client = newClient();
        val writer = newParentChildDocumentWriter(client)) {
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

        do {
          hitProcessorMonitor.start();
          for (val hit : resp.getHits()) {
            val pair = EsVariantCallPair.builder()
                .variant(
                    VARIANT_CONVERTER.convertFromSearchHit(hit));

            for (val innerHit : hit.getInnerHits().get("call")) {
              pair.call(CALL_CONVERTER.convertFromSearchHit(innerHit));
              hitProcessorMonitor.incr();
            }
            pairList.add(pair.build());
          }
          resp = client.prepareSearchScroll(resp.getScrollId())
              .setScroll(TimeValue.timeValueMinutes(3))
              .get();
          hasHits = resp.getHits().getTotalHits() > 0;
          pairList.clear();
          count++;
        } while (hasHits && count < maxIterations);
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
