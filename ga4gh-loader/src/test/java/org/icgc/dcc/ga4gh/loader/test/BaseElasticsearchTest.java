/*
 * Copyright (c) 2016 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.ga4gh.loader.test;

import com.carrotsearch.randomizedtesting.annotations.SeedDecorators;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.io.Resources;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.ga4gh.loader.Config;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.icgc.dcc.common.es.security.SecurityManagerWorkaroundSeedDecorator;
import org.junit.Before;

import java.io.File;

import static com.google.common.base.Preconditions.checkState;
import static org.icgc.dcc.ga4gh.common.resources.TypeNames.VARIANT;
import static org.icgc.dcc.common.core.json.Jackson.DEFAULT;
import static org.icgc.dcc.common.core.json.JsonNodeBuilders.object;

@Slf4j
@SeedDecorators(value = SecurityManagerWorkaroundSeedDecorator.class)
@ClusterScope(scope = Scope.TEST, numDataNodes = 1, maxNumDataNodes = 1, supportsDedicatedMasters = false, transportClientRatio = 0.0)
public abstract class BaseElasticsearchTest extends ESIntegTestCase {

  /**
   * Constants.
   */
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final ObjectReader READER = MAPPER.readerFor(ObjectNode.class);

  /**
   * Test configuration.
   */
  private static final String MAPPINGS_DIR = "org/icgc/dcc/ga4gh/resources/mappings";
  private static final String FIXTURES_DIR = "src/test/resources/fixtures";

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  protected void createIndex() {
    log.info("Creating index...");
    checkState(prepareCreate(Config.PARENT_CHILD_INDEX_NAME)
        .setSettings(object(read("index.settings.json"))
            .with("index.number_of_shards", 1)
            .with("index.number_of_replicas", 0).end().toString())
        .addMapping("callset", read("callset.mapping.json").toString())
        .addMapping("variant", read("variant.mapping.json").toString())
        .execute().actionGet().isAcknowledged());
  }

  protected void indexData() {
    indexData(this.getClass());
  }

  @SneakyThrows
  protected void indexData(Class<?> testClass) {
    val dataFile = new File(FIXTURES_DIR, testClass.getSimpleName() + ".txt");
    log.info("Loading data from: {}", dataFile);

    val iterator = READER.readValues(dataFile);
    while (iterator.hasNext()) {
      val docMetadata = (ObjectNode) iterator.next();
      val indexMetadata = docMetadata.get("index");
      val indexName = indexMetadata.get("_index").textValue();
      val indexType = indexMetadata.get("_type").textValue();
      val indexId = indexMetadata.get("_id").textValue();
      checkState(iterator.hasNext(), "Incorrect format of input test data file. Expected data after document metadata");
      val doc = (ObjectNode) iterator.next();
      val indexRequest = client().prepareIndex(indexName, indexType, indexId).setSource(doc.toString());
      indexRandom(true, indexRequest);
    }
  }

  protected GetResponse getVariant(@NonNull String variantId) {
    return prepareGet().setType(VARIANT).setId(variantId).get();
  }

  protected GetRequestBuilder prepareGet() {
    return client().prepareGet().setIndex(Config.PARENT_CHILD_INDEX_NAME);
  }

  @SneakyThrows
  private static ObjectNode read(String fileName) {
    val url = Resources.getResource(MAPPINGS_DIR + "/" + fileName);
    return (ObjectNode) DEFAULT.readTree(url);
  }

}
