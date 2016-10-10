package org.collaboratory.ga4gh.loader;

import static org.collaboratory.ga4gh.loader.Config.ES_URL;
import static org.collaboratory.ga4gh.loader.Config.INDEX_NAME;
import static org.collaboratory.ga4gh.loader.Config.NODE_ADDRESS;
import static org.collaboratory.ga4gh.loader.Config.NODE_PORT;
import static org.icgc.dcc.dcc.common.es.DocumentWriterFactory.createDocumentWriter;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.icgc.dcc.dcc.common.es.DocumentWriterConfiguration;
import org.icgc.dcc.dcc.common.es.core.DocumentWriter;

import lombok.val;

public class Factory {

  public static Client newClient() {
    val client = new TransportClient();
    client.addTransportAddress(new InetSocketTransportAddress(NODE_ADDRESS, NODE_PORT));

    return client;
  }

  public static DocumentWriter newDocumentWriter() {
    return createDocumentWriter(new DocumentWriterConfiguration().esUrl(ES_URL).indexName(INDEX_NAME));
  }

  public static Loader newLoader(Client client, DocumentWriter writer) {
    val indexer = new Indexer(client, writer, INDEX_NAME);

    return new Loader(indexer);
  }

}
