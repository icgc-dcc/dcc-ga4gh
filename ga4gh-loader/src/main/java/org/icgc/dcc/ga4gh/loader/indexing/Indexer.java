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

package org.icgc.dcc.ga4gh.loader.indexing;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.experimental.NonFinal;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.elasticsearch.client.Client;
import org.elasticsearch.rest.RestStatus;
import org.icgc.dcc.dcc.common.es.core.DocumentWriter;
import org.icgc.dcc.dcc.common.es.impl.IndexDocumentType;
import org.icgc.dcc.dcc.common.es.json.JacksonFactory;
import org.icgc.dcc.dcc.common.es.model.IndexDocument;
import org.icgc.dcc.ga4gh.common.model.converters.EsCallSetConverterJson;
import org.icgc.dcc.ga4gh.common.model.converters.EsConsensusCallConverterJson;
import org.icgc.dcc.ga4gh.common.model.converters.EsVariantCallPairConverterJson;
import org.icgc.dcc.ga4gh.common.model.converters.EsVariantConverterJson;
import org.icgc.dcc.ga4gh.common.model.converters.EsVariantSetConverterJson;
import org.icgc.dcc.ga4gh.common.model.es.EsCallSet;
import org.icgc.dcc.ga4gh.common.model.es.EsConsensusCall;
import org.icgc.dcc.ga4gh.common.model.es.EsVariantSet;
import org.icgc.dcc.ga4gh.loader.Config;
import org.icgc.dcc.ga4gh.loader.utils.counting.CounterMonitor;
import org.icgc.dcc.ga4gh.loader.utils.idstorage.id.impl.VariantIdContext;
import org.icgc.dcc.ga4gh.loader.utils.idstorage.storage.MapStorage;

import java.util.function.BiConsumer;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.propagate;
import static java.lang.String.format;
import static org.elasticsearch.common.xcontent.XContentType.SMILE;
import static org.icgc.dcc.ga4gh.common.PropertyNames.VARIANT_SET_ID;
import static org.icgc.dcc.ga4gh.common.TypeNames.CALL;
import static org.icgc.dcc.ga4gh.common.TypeNames.CALL_SET;
import static org.icgc.dcc.ga4gh.common.TypeNames.VARIANT;
import static org.icgc.dcc.ga4gh.common.TypeNames.VARIANT_SET;
import static org.icgc.dcc.ga4gh.common.TypeNames.VCF_HEADER;
import static org.icgc.dcc.ga4gh.loader.Config.MONITOR_INTERVAL_COUNT;
import static org.icgc.dcc.ga4gh.common.types.IndexModes.NESTED;
import static org.icgc.dcc.ga4gh.common.types.IndexModes.PARENT_CHILD;
import static org.icgc.dcc.ga4gh.loader.utils.counting.CounterMonitor.createCounterMonitor;

@Slf4j
@RequiredArgsConstructor
public class Indexer {

  /**
   * Constants.
   */
  private static final int MAX_NUM_SEGMENTS = 1;
  private static final ObjectWriter BINARY_WRITER = JacksonFactory.getObjectWriter();
  private static final EsVariantSetConverterJson VARIANT_SET_CONVERTER_JSON = new EsVariantSetConverterJson();
  private static final EsCallSetConverterJson CALL_SET_CONVERTER_JSON = new EsCallSetConverterJson();
  private static final EsVariantConverterJson VARIANT_CONVERTER_JSON = new EsVariantConverterJson();
  private static final EsConsensusCallConverterJson CONSENSUS_CALL_CONVERTER_JSON = new EsConsensusCallConverterJson();
  private static final EsVariantCallPairConverterJson VARIANT_CALL_PAIR_CONVERTER_JSON = new EsVariantCallPairConverterJson(
      VARIANT_CONVERTER_JSON,CONSENSUS_CALL_CONVERTER_JSON,
      VARIANT_CONVERTER_JSON,CONSENSUS_CALL_CONVERTER_JSON);


  /**
   * Dependencies.
   */
  @NonNull
  private final Client client;
  @NonNull
  private final DocumentWriter writer;

  /**
   * Configuration.
   */
  @NonNull
  private final IndexCreatorContext indexCreatorContext;

  /*
   * State
   */
  @NonFinal private IndexCreator indexCreator = null;
  private long callId = 0;
  private final CounterMonitor variantMonitor = createCounterMonitor("VariantIndexing", MONITOR_INTERVAL_COUNT);
  private final CounterMonitor vcfHeaderMonitor = createCounterMonitor("VCFHeaderIndexing", MONITOR_INTERVAL_COUNT);

  @SneakyThrows
  public void prepareIndex() {
      lazyInitIndexCreator();
      indexCreator.execute();
  }

  public void optimize(){
    lazyInitIndexCreator();
    forceMergeAllIndices(MAX_NUM_SEGMENTS);
    flushAllIndices();
  }

  @SneakyThrows
  public void indexVariants(@NonNull Stream<VariantIdContext<Long>> stream){
    variantMonitor.start();
    stream.forEach(this::writeVariant);
    variantMonitor.stop();
  }

  @SneakyThrows
  public void indexVariantSets(@NonNull MapStorage<EsVariantSet, Integer> variantSetMapStorage) {
    indexMapStorage(variantSetMapStorage, this::writeVariantSet);
  }

  @SneakyThrows
  public void indexCallSets(@NonNull MapStorage<EsCallSet, Integer> callSetMapStorage) {
    indexMapStorage(callSetMapStorage, this::writeCallSet);
  }

  private void lazyInitIndexCreator(){
    if (indexCreator == null){
      indexCreator = new IndexCreator(indexCreatorContext);
    }
  }

  private void flushAllIndices(){
    log.info("Started paranoid flush...");
    indexCreatorContext.getClient()
        .admin().indices()
        .prepareFlush()
        .execute();
    log.info("Finished flush");
  }

  @SneakyThrows
  private void  forceMergeAllIndices(final int maxNumSegments){
    log.info("Starting force merge on all indices...");
    val client = indexCreatorContext.getClient();
    val request = client.admin().indices().prepareForceMerge().setMaxNumSegments(maxNumSegments);
    val response  = request.execute().get();
    int numFailedShards = response.getFailedShards();
    checkState(numFailedShards==0, "The request to forceMerge all indices to {} segments, had {} failed shards", maxNumSegments, numFailedShards);
    log.info("Finished force merge on all indices");
  }


  private <K, ID> void indexMapStorage(MapStorage<K, ID> mapStorage, BiConsumer<K, ID> consumer){
    mapStorage.getMap().forEach(consumer);
  }

  private String nextCallId() {
    return Long.toString(++callId);
  }

  @SneakyThrows
  private void writeVariant(VariantIdContext<Long> variantIdContext){
    val variantId = variantIdContext.getId().toString();
    val esVariantCallPair = variantIdContext.getEsVariantCallPair();
    ObjectNode variantJsonData = null;
    if (indexCreatorContext.getIndexMode() == NESTED){
      variantJsonData = VARIANT_CALL_PAIR_CONVERTER_JSON.convertToObjectNode(esVariantCallPair);
      writer.write(new IndexDocument(variantId, variantJsonData, new VariantDocumentType()));
    } else if (indexCreatorContext.getIndexMode() == PARENT_CHILD){
      variantJsonData = VARIANT_CONVERTER_JSON.convertToObjectNode(esVariantCallPair.getVariant());
      writer.write(new IndexDocument(variantId, variantJsonData, new VariantDocumentType()));
      esVariantCallPair.getCalls().forEach(x -> writeCall(variantId, x));
    } else {
      throw new IllegalStateException(
          format("The indexMode [%s] is not implemented",
              indexCreatorContext.getIndexMode().name()));
    }
    variantMonitor.preIncr();
  }

  @SneakyThrows
  private void writeCall(String parentVariantId, EsConsensusCall call){
    writer.write(new IndexDocument(nextCallId(), CONSENSUS_CALL_CONVERTER_JSON.convertToObjectNode(call), new CallDocumentType(),
        parentVariantId));
  }

  @SneakyThrows
  private void writeCallSet(@NonNull EsCallSet callSet, @NonNull Integer callSetId) {
    val data = CALL_SET_CONVERTER_JSON.convertToObjectNode(callSet);
    writer.write( new IndexDocument(callSetId.toString(),data, new CallSetDocumentType()));
  }

  @SneakyThrows
  private void writeVariantSet(@NonNull EsVariantSet variantSet, @NonNull Integer variantSetId) {
    val data = VARIANT_SET_CONVERTER_JSON.convertToObjectNode(variantSet);
    writer.write(new IndexDocument(variantSetId.toString(), data, new VariantSetDocumentType()));
  }

  // TODO: [rtisma] rethink how will organize this dao
  @SneakyThrows
  public void indexVCFHeader(final String objectId, @NonNull final ObjectNode vcfHeader) {
    val parent_variant_set_id = vcfHeader.path(VARIANT_SET_ID).textValue();
    checkState(
        client.prepareIndex(Config.PARENT_CHILD_INDEX_NAME, VCF_HEADER, objectId)
            .setContentType(SMILE)
            .setSource(createSource(vcfHeader))
            .setParent(parent_variant_set_id)
            .setRouting(VARIANT)
            .get().status().equals(RestStatus.CREATED));
  }

  private static byte[] createSource(@NonNull final Object document) {
    try {
      return BINARY_WRITER.writeValueAsBytes(document);
    } catch (JsonProcessingException e) {
      throw propagate(e);
    }
  }

  public static Indexer createIndexer(Client client, DocumentWriter writer,
      IndexCreatorContext indexCreatorContext) {
    return new Indexer(client, writer, indexCreatorContext);
  }

  private static class VariantDocumentType implements IndexDocumentType {

    @Override
    public String getIndexType() {
      return VARIANT;
    }
  }

  private static class VariantSetDocumentType implements IndexDocumentType {

    @Override
    public String getIndexType() {
      return VARIANT_SET;
    }

  }

  private static class CallSetDocumentType implements IndexDocumentType {

    @Override
    public String getIndexType() {
      return CALL_SET;
    }
  }

  private static class CallDocumentType implements IndexDocumentType {

    @Override
    public String getIndexType() {
      return CALL;
    }
  }

}
