package org.collaboratory.ga4gh.loader;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.io.Resources.getResource;
import static org.collaboratory.ga4gh.loader.Config.ASCENDING_MODE;
import static org.collaboratory.ga4gh.loader.Config.BULK_NUM_THREADS;
import static org.collaboratory.ga4gh.loader.Config.BULK_SIZE_MB;
import static org.collaboratory.ga4gh.loader.Config.DATA_FETCHER_MAX_FILESIZE_BYTES;
import static org.collaboratory.ga4gh.loader.Config.DATA_FETCHER_SHUFFLE;
import static org.collaboratory.ga4gh.loader.Config.DATA_FETCHER_SOMATIC_SSMS_ONLY;
import static org.collaboratory.ga4gh.loader.Config.DEFAULT_FILE_META_DATA_STORE_FILENAME;
import static org.collaboratory.ga4gh.loader.Config.INDEX_NAME;
import static org.collaboratory.ga4gh.loader.Config.NODE_ADDRESS;
import static org.collaboratory.ga4gh.loader.Config.NODE_PORT;
import static org.collaboratory.ga4gh.loader.Config.SORT_MODE;
import static org.collaboratory.ga4gh.loader.Config.STORAGE_BYPASS_MD5_CHECK;
import static org.collaboratory.ga4gh.loader.Config.STORAGE_OUTPUT_VCF_STORAGE_DIR;
import static org.collaboratory.ga4gh.loader.Config.STORAGE_PERSIST_MODE;
import static org.collaboratory.ga4gh.loader.Config.USE_MAP_DB;
import static org.collaboratory.ga4gh.loader.Config.USE_STRING_ES_VARIANT_MODEL;
import static org.collaboratory.ga4gh.loader.model.metadata.FileMetaDataFetcher.generateSeed;
import static org.icgc.dcc.dcc.common.es.DocumentWriterFactory.createDocumentWriter;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Properties;

import org.collaboratory.ga4gh.loader.factory.IdCacheFactory;
import org.collaboratory.ga4gh.loader.factory.IdDiskCacheFactory;
import org.collaboratory.ga4gh.loader.factory.IdRamCacheFactory;
import org.collaboratory.ga4gh.loader.model.es.EsVariant;
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

  private static final String TRANSPORT_SETTINGS_FILENAME =
      "org/collaboratory/ga4gh/resources/settings/transport.properties";

  private static Properties newResourceProperties(final String filename) throws IOException {
    val uri = getResource(filename);
    val prop = new Properties();
    prop.load(uri.openStream());
    return prop;
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

  public static <T extends EsVariant> IdCacheFactory<T> newIdCacheFactory(boolean useMapDB,
      boolean useVariantStringImpl) {
    val defaultInitId = 1L;
    val defaultStorageDirname = "target";
    checkState(!useMapDB || !useVariantStringImpl,
        "Cannot cast EsStringVariant to EsByteVariant. IdDiskCacheFactory is not configured for EsStringVariant since there is no serializer");
    if (useMapDB) {
      return (IdCacheFactory<T>) new IdDiskCacheFactory(defaultStorageDirname, defaultInitId);
    } else {
      return new IdRamCacheFactory<T>(defaultInitId);
    }
  }

  public static <T extends EsVariant> IdCacheFactory<T> newIdCacheFactory() {
    return newIdCacheFactory(USE_MAP_DB, USE_STRING_ES_VARIANT_MODEL);
  }

  public static void main(String[] args) throws IOException {
    boolean useMapDB = false;
    boolean useString = false;

    IdCacheFactory<EsVariant> idMapDB = newIdCacheFactory(useMapDB, useString);
    idMapDB.build();
    long start = System.currentTimeMillis();
    for (int i = 0; i < 4000000; i++) {
      EsVariant byteVar = EsVariant.newEsVariantBuilder(useString)
          .start(1)
          .end(2 + i)
          .alternativeBase("A")
          .alternativeBase("B")
          .referenceBases("AC")
          .referenceName("chrom1")
          .build();
      idMapDB.getVariantIdCache().add(byteVar);
      log.info("" + i);
    }
    long dur = System.currentTimeMillis() - start;

    for (val vars : idMapDB.getVariantIdCache().getReverseCache().values()) {
      // log.info("Id: {} {}", idMapDB.getVariantIdCache().getIdAsString(vars), vars.toString());
    }
    log.info("duration = {}", dur);

  }

  public static Indexer newIndexer(Client client, DocumentWriter writer,
      IdCacheFactory<EsVariant> idCacheFactory)
      throws Exception {
    return new Indexer(client, writer, INDEX_NAME,
        idCacheFactory.getVariantIdCache(),
        idCacheFactory.getVariantSetIdCache(),
        idCacheFactory.getCallSetIdCache());
  }

  public static Loader newLoader(Client client, DocumentWriter writer, IdCacheFactory<EsVariant> idCacheFactory)
      throws Exception {
    return new Loader(newIndexer(client, writer, idCacheFactory), newStorage(), USE_STRING_ES_VARIANT_MODEL);
  }

  public static Storage newStorage() {
    return new Storage(STORAGE_PERSIST_MODE, STORAGE_OUTPUT_VCF_STORAGE_DIR, STORAGE_BYPASS_MD5_CHECK);
  }

  public static FileMetaDataFetcher newFileMetaDataFetcher() {
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
        .maxFileSizeBytes(DATA_FETCHER_MAX_FILESIZE_BYTES)
        .build();
  }

}
