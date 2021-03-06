/*
 * Copyright (c) 2017 The Ontario Institute for Cancer Research. All rights reserved.
 *
 * This program and the accompanying materials are made available under the terms of the GNU Public License v3.0.
 * You should have received a copy of the GNU General Public License along with
 * this program. If not, see <http://www.gnu.org/licenses/>.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT
 * SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER
 * IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.icgc.dcc.ga4gh.loader;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Consumer;

import static com.google.common.base.Stopwatch.createStarted;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.icgc.dcc.ga4gh.loader.Config.PARENT_CHILD_INDEX_NAME;
import static org.icgc.dcc.ga4gh.loader.factory.Factory.newClient;

@Slf4j
@RequiredArgsConstructor
public class Benchmarks {

  @NonNull
  private final Client client;

  @NonNull
  private final String indexName;

  @SneakyThrows
  public static void main(String[] args) {
    try (val client = newClient()) {
      val benchmarks = new Benchmarks(client, PARENT_CHILD_INDEX_NAME);
      benchmarks.execute();
    } catch (Exception e) {
      log.error("Exception running: ", e);
    }
  }

  public void execute() {
    count(rangeQuery("start").from(10_000_000).to(20_000_000));
    terms(b -> b.field("chr"));
    terms(b -> b.field("type"));
    terms(b -> b.field("alternateAlleles.baseString"));
    histogram(b -> b.field("start").interval(1_000_000));
  }

  @SneakyThrows
  private void terms(Consumer<TermsAggregationBuilder> builder) {
    val aggregation = AggregationBuilders.terms("aggregation");
    builder.accept(aggregation);
    display((Terms) aggregate(aggregation).get("aggregation"));
  }

  @SneakyThrows
  private void histogram(Consumer<HistogramAggregationBuilder> builder) {
    val aggregation = AggregationBuilders.histogram("aggregation");
    builder.accept(aggregation);
    display((Histogram) aggregate(aggregation).get("aggregation"));
  }

  @SneakyThrows
  private void count(QueryBuilder builder) {
    val query = client.prepareSearch(indexName).setSource(new SearchSourceBuilder().size(0).query(builder));

    log.info(">>> Executing counter: {}", builder);
    val watch = createStarted();
    val response = query.execute().get();
    log.info("<<< Took: {}", watch);

    log.info("Count: {}", response.getHits().getTotalHits());
  }

  @SneakyThrows
  private Aggregations aggregate(AggregationBuilder aggregation) {
    val query = client.prepareSearch(indexName).addAggregation(aggregation);

    log.info(">>> Executing {}", query);
    val watch = createStarted();
    val response = query.execute().get();
    log.info("<<< Took: {}", watch);

    return response.getAggregations();
  }

  private void display(MultiBucketsAggregation aggregation) {
    for (val bucket : aggregation.getBuckets()) {
      log.info("{} = {}", bucket.getKey(), bucket.getDocCount());
    }
  }

  public static void writeToNewFile(final String filename, final String message) {
    writeToFile(filename, message, true);
  }

  public static void writeToAppendFile(final String filename, final String message) {
    writeToFile(filename, message, false);
  }

  @SneakyThrows
  public static void writeToFile(final String filename, final String message, final boolean overwrite) {
    val writer = new PrintWriter(filename);
    val path = Paths.get(filename);
    Path dir = path.getParent();
    if (dir == null) {
      dir = Paths.get("./");
    }

    val dirDoesNotExist = !Files.exists(dir);
    if (dirDoesNotExist) {
      Files.createDirectories(dir);
    }
    writer.write(message);
    writer.close();
  }


}
