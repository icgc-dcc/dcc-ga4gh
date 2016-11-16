package org.collaboratory.ga4gh.loader;

import static org.collaboratory.ga4gh.loader.Config.INDEX_NAME;
import static org.collaboratory.ga4gh.loader.Config.NODE_ADDRESS;
import static org.collaboratory.ga4gh.loader.Config.NODE_PORT;

import java.net.InetAddress;
import java.util.Random;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.icgc.dcc.dcc.common.es.DocumentWriterConfiguration;
import org.icgc.dcc.dcc.common.es.core.DocumentWriter;
import org.icgc.dcc.dcc.common.es.impl.BulkProcessorListener;
import org.icgc.dcc.dcc.common.es.impl.ClusterStateVerifier;
import org.icgc.dcc.dcc.common.es.impl.DefaultDocumentWriter;
import org.icgc.dcc.dcc.common.es.impl.IndexingState;

import lombok.SneakyThrows;
import lombok.val;

public class Factory {

  @SuppressWarnings("resource")
  @SneakyThrows
  // TODO: rtisma -- put this in a common module, so that every one can reference
  public static Client newClient() {
    val client = new PreBuiltTransportClient(Settings.EMPTY)
        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(NODE_ADDRESS), NODE_PORT));

    return client;
  }

  // TODO: rtisma -- [HACK] copied from icgc.dcc. Temporary to get things working, but icgc.dcc library needs to be
  // updated to ES 5.0.0
  public static DocumentWriter createDocumentWriter(DocumentWriterConfiguration configuration) {
    val client = configuration.client() != null ? configuration.client() : newClient();
    val writerId = createWriterId();
    val indexingState = new IndexingState(writerId);
    val clusterStateVerifier = new ClusterStateVerifier(client, configuration.indexName(), writerId, indexingState);
    val bulkProcessorListener = new BulkProcessorListener(clusterStateVerifier, indexingState, writerId);
    val bulkProcessor = createProcessor(client, bulkProcessorListener);
    return new DefaultDocumentWriter(client, configuration.indexName(), indexingState, bulkProcessor, writerId);
  }

  // TODO: rtisma -- [HACK] copied from icgc.dcc. Temporary to get things working, but icgc.dcc library needs to be
  // updated to ES 5.0.0
  private static BulkProcessor createProcessor(Client client, BulkProcessorListener listener) {
    final int BULK_ACTIONS = -1; // Unlimited
    val bulkProcessor =
        BulkProcessor.builder(client, listener).setBulkActions(BULK_ACTIONS)
            .setBulkSize(DefaultDocumentWriter.BULK_SIZE)
            .setConcurrentRequests(0).build();

    // Need to give back reference to bulkProcessor as it's reused for re-indexing of failed requests.
    listener.setProcessor(bulkProcessor);

    return bulkProcessor;
  }

  // TODO: rtisma -- [HACK] copied from icgc.dcc. Temporary to get things working, but icgc.dcc library needs to be
  // updated to ES 5.0.0
  private static String createWriterId() {
    final Random RANDOM_GENERATOR = new Random();
    val id = RANDOM_GENERATOR.nextInt(Integer.MAX_VALUE);

    return String.valueOf(Math.abs(id));
  }

  public static DocumentWriter newDocumentWriter(final Client client) {
    return createDocumentWriter(new DocumentWriterConfiguration().client(client).indexName(INDEX_NAME));
  }

  public static Loader newLoader(Client client, DocumentWriter writer) {
    val indexer = new Indexer(client, writer, INDEX_NAME);

    return new Loader(indexer);
  }

}
