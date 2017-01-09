package org.collaboratory.ga4gh.loader;

import static org.collaboratory.ga4gh.loader.Config.INDEX_NAME;
import static org.collaboratory.ga4gh.loader.Config.NODE_ADDRESS;
import static org.collaboratory.ga4gh.loader.Config.NODE_PORT;
import static org.icgc.dcc.dcc.common.es.DocumentWriterFactory.createDocumentWriter;

import java.net.InetAddress;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.icgc.dcc.dcc.common.es.DocumentWriterConfiguration;
import org.icgc.dcc.dcc.common.es.core.DocumentWriter;

import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Factory {

  private static final int NUM_THREADS = 3;
  private static final int BULK_SIZE_MB = 100;
  private static final boolean PERSIST_MODE = true;
  private static final String OUTPUT_VCF_STORAGE_DIR = "target/storedVCFs";

  @SuppressWarnings("resource")
  @SneakyThrows
  // TODO: rtisma -- put this in a common module, so that every one can reference
  public static Client newClient() {
    val client = new PreBuiltTransportClient(Settings.EMPTY)
        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(NODE_ADDRESS), NODE_PORT));

    return client;
  }

  public static DocumentWriter newDocumentWriter(final Client client) {
    return createDocumentWriter(new DocumentWriterConfiguration()
        .client(client)
        .indexName(INDEX_NAME)
        .bulkSizeMb(BULK_SIZE_MB)
        .threadsNum(NUM_THREADS));
  }

  public static Loader newLoader(Client client, DocumentWriter writer) {
    val indexer = new Indexer(client, writer, INDEX_NAME);
    val storage = newStorage();
    return new Loader(indexer, storage);
  }

  public static Storage newStorage() {
    return new Storage(PERSIST_MODE, OUTPUT_VCF_STORAGE_DIR);
  }
}
