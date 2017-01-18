package org.collaboratory.ga4gh.loader;

import static com.google.common.io.Resources.getResource;
import static org.collaboratory.ga4gh.loader.Config.ASCENDING_MODE;
import static org.collaboratory.ga4gh.loader.Config.BULK_NUM_THREADS;
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
import static org.collaboratory.ga4gh.loader.Config.OUTPUT_VCF_STORAGE_DIR;
import static org.collaboratory.ga4gh.loader.Config.PERSIST_MODE;
import static org.collaboratory.ga4gh.loader.Config.SORT_MODE;
import static org.collaboratory.ga4gh.loader.Config.USE_HASH_CODE;
import static org.collaboratory.ga4gh.loader.Config.USE_MAP_DB;
import static org.collaboratory.ga4gh.loader.model.metadata.FileMetaDataFetcher.generateSeed;
import static org.icgc.dcc.dcc.common.es.DocumentWriterFactory.createDocumentWriter;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;
import java.util.Properties;

import org.collaboratory.ga4gh.loader.model.metadata.FileMetaDataFetcher;
import org.collaboratory.ga4gh.loader.utils.IdCache;
import org.collaboratory.ga4gh.loader.utils.IdCacheImpl;
import org.collaboratory.ga4gh.loader.utils.IdHashCodeCache;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.icgc.dcc.dcc.common.es.DocumentWriterConfiguration;
import org.icgc.dcc.dcc.common.es.core.DocumentWriter;

import com.google.common.collect.Maps;

import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Factory {

  private static final String TRANSPORT_SETTINGS_FILENAME =
      "org/collaboratory/ga4gh/resources/settings/transport.properties";

  private static Properties newResourceProperties(final String filename) throws IOException {
    val uri = getResource(filename);
    val prop = new Properties();
    prop.load(uri.openStream());
    return prop;
  }

  public static Settings newEmptySettings() throws IOException {
    return Settings.EMPTY;
  }

  public static Settings newSettings() throws IOException {
    val settingsProp = newResourceProperties(TRANSPORT_SETTINGS_FILENAME);
    return Settings.builder()
        .put(settingsProp)
        .build();
  }

  @SuppressWarnings("resource")
  @SneakyThrows
  // TODO: rtisma -- put this in a common module, so that every one can reference
  public static Client newClient() {
    val settings = newSettings();
    val client = new PreBuiltTransportClient(settings)
        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(NODE_ADDRESS), NODE_PORT));

    return client;
  }

  public static DocumentWriter newDocumentWriter(final Client client) {
    return createDocumentWriter(new DocumentWriterConfiguration()
        .client(client)
        .indexName(INDEX_NAME)
        .bulkSizeMb(BULK_SIZE_MB)
        .threadsNum(BULK_NUM_THREADS));
  }

  public static IdCache<String> newIdCache(final long init_id, boolean useHashCode,
      boolean useMapDB) {
    if (useHashCode) {
      return new IdHashCodeCache<String>(IdCacheImpl.<Integer> newIdCache(newMap(useMapDB), init_id));
    } else {
      return IdCacheImpl.<String> newIdCache(newMap(useMapDB), init_id);
    }
  }

  public static <K, V> Map<K, V> newMap(boolean useMapDB) {
    Map<K, V> cache;
    if (useMapDB) {
      log.info("sdfsdfsdf");
      cache = Maps.newHashMap();
    } else {
      cache = Maps.newHashMap();
    }
    return cache;
  }

  public static Indexer newIndexer(Client client, DocumentWriter writer, boolean useMapDB) {
    return new Indexer(client, writer, INDEX_NAME,
        newIdCache(1L, USE_HASH_CODE, USE_MAP_DB),
        newIdCache(1L, USE_HASH_CODE, USE_MAP_DB),
        newIdCache(1L, USE_HASH_CODE, USE_MAP_DB),
        newIdCache(1L, USE_HASH_CODE, USE_MAP_DB));
  }

  public static Loader newLoader(Client client, DocumentWriter writer) {
    return new Loader(newIndexer(client, writer, USE_MAP_DB), newStorage());
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
        .sort(SORT_MODE)
        .ascending(ASCENDING_MODE)
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
