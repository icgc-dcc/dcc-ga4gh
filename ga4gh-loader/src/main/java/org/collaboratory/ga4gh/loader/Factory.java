package org.collaboratory.ga4gh.loader;

import static org.collaboratory.ga4gh.loader.Config.BULK_SIZE_MB;
import static org.collaboratory.ga4gh.loader.Config.DATA_FETCHER_LIMIT;
import static org.collaboratory.ga4gh.loader.Config.DATA_FETCHER_MAX_FILESIZE_BYTES;
import static org.collaboratory.ga4gh.loader.Config.DATA_FETCHER_NUM_DONORS;
import static org.collaboratory.ga4gh.loader.Config.DATA_FETCHER_SHUFFLE;
import static org.collaboratory.ga4gh.loader.Config.DATA_FETCHER_SOMATIC_SSMS_ONLY;
import static org.collaboratory.ga4gh.loader.Config.DEFAULT_FILE_META_DATA_STORE_FILENAME;
import static org.collaboratory.ga4gh.loader.Config.INDEX_NAME;
import static org.collaboratory.ga4gh.loader.Config.NODE_ADDRESS;
import static org.collaboratory.ga4gh.loader.Config.NODE_PORT;
import static org.collaboratory.ga4gh.loader.Config.NUM_THREADS;
import static org.collaboratory.ga4gh.loader.Config.OUTPUT_VCF_STORAGE_DIR;
import static org.collaboratory.ga4gh.loader.Config.PERSIST_MODE;
import static org.collaboratory.ga4gh.loader.model.metadata.FileMetaDataFetcher.generateSeed;
import static org.icgc.dcc.dcc.common.es.DocumentWriterFactory.createDocumentWriter;

import java.net.InetAddress;

import org.collaboratory.ga4gh.loader.model.metadata.FileMetaDataFetcher;
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

  private static final String FILE_META_DATA_STORE_FILENAME = "allFileMetaDatas.bin";

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

  public static FileMetaDataFetcher newFileMetaDataFetcherCustom() {
    long seed = generateSeed();
    if (DATA_FETCHER_SHUFFLE) {
      log.info("Using seed [{}] for FileMetaDataFetcher instance", seed);
    }
    return FileMetaDataFetcher.builder()
        .shuffle(DATA_FETCHER_SHUFFLE)
        .maxFileSizeBytes(DATA_FETCHER_MAX_FILESIZE_BYTES)
        .seed(seed)
        .numDonors(DATA_FETCHER_NUM_DONORS)
        .somaticSSMsOnly(DATA_FETCHER_SOMATIC_SSMS_ONLY)
        .limit(DATA_FETCHER_LIMIT)
        .build();
  }

  public static FileMetaDataFetcher newFileMetaDataFetcherAll() {
    long seed = generateSeed();
    if (DATA_FETCHER_SHUFFLE) {
      log.info("Using seed [{}] for FileMetaDataFetcher instance", seed);
    }
    return FileMetaDataFetcher.builder()
        .shuffle(DATA_FETCHER_SHUFFLE)
        .seed(seed)
        .fromFilename(DEFAULT_FILE_META_DATA_STORE_FILENAME)
        .somaticSSMsOnly(DATA_FETCHER_SOMATIC_SSMS_ONLY)
        .build();
  }

  public static FileMetaDataFetcher newFileMetaDataFetcherFirst10Biggest() {
    long seed = generateSeed();
    if (DATA_FETCHER_SHUFFLE) {
      log.info("Using seed [{}] for FileMetaDataFetcher instance", seed);
    }
    return FileMetaDataFetcher.builder()
        .sort(true)
        .somaticSSMsOnly(DATA_FETCHER_SOMATIC_SSMS_ONLY)
        .ascending(true)
        .limit(10)
        .build();
  }
}
