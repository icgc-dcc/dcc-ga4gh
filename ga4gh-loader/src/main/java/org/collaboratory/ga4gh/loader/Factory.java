package org.collaboratory.ga4gh.loader;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.io.Resources.getResource;
import static org.collaboratory.ga4gh.loader.Config.ASCENDING_MODE;
import static org.collaboratory.ga4gh.loader.Config.BULK_NUM_THREADS;
import static org.collaboratory.ga4gh.loader.Config.BULK_SIZE_MB;
import static org.collaboratory.ga4gh.loader.Config.DATA_FETCHER_LIMIT;
import static org.collaboratory.ga4gh.loader.Config.DATA_FETCHER_MAX_FILESIZE_BYTES;
import static org.collaboratory.ga4gh.loader.Config.DATA_FETCHER_SHUFFLE;
import static org.collaboratory.ga4gh.loader.Config.DATA_FETCHER_SOMATIC_SSMS_ONLY;
import static org.collaboratory.ga4gh.loader.Config.DEFAULT_FILE_META_DATA_STORE_FILENAME;
import static org.collaboratory.ga4gh.loader.Config.NESTED_INDEX_NAME;
import static org.collaboratory.ga4gh.loader.Config.NESTED_SCROLL_SIZE;
import static org.collaboratory.ga4gh.loader.Config.NODE_ADDRESS;
import static org.collaboratory.ga4gh.loader.Config.NODE_PORT;
import static org.collaboratory.ga4gh.loader.Config.PARENT_CHILD_INDEX_NAME;
import static org.collaboratory.ga4gh.loader.Config.SORT_MODE;
import static org.collaboratory.ga4gh.loader.Config.STORAGE_BYPASS_MD5_CHECK;
import static org.collaboratory.ga4gh.loader.Config.STORAGE_OUTPUT_VCF_STORAGE_DIR;
import static org.collaboratory.ga4gh.loader.Config.STORAGE_PERSIST_MODE;
import static org.collaboratory.ga4gh.loader.Config.USE_MAP_DB;
import static org.collaboratory.ga4gh.loader.model.metadata.FileMetaDataFetcher.generateSeed;
import static org.collaboratory.ga4gh.loader.vcf.CallProcessorManager.newCallProcessorManager;
import static org.collaboratory.ga4gh.loader.vcf.processors.BasicCallProcessor.newUnFilteredBasicCallProcessor;
import static org.collaboratory.ga4gh.loader.vcf.processors.DummyCallProcessor.newDummyCallProcessor;
import static org.icgc.dcc.dcc.common.es.DocumentWriterFactory.createDocumentWriter;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Properties;

import org.collaboratory.ga4gh.loader.factory.IdCacheFactory;
import org.collaboratory.ga4gh.loader.factory.IdMixedCacheFactory;
import org.collaboratory.ga4gh.loader.factory.IdRamCacheFactory;
import org.collaboratory.ga4gh.loader.indexing.IndexCreatorContext;
import org.collaboratory.ga4gh.loader.indexing.Indexer;
import org.collaboratory.ga4gh.loader.indexing.ParentChild2NestedIndexConverter;
import org.collaboratory.ga4gh.loader.model.es.converters.EsCallConverter;
import org.collaboratory.ga4gh.loader.model.es.converters.EsCallSetConverter;
import org.collaboratory.ga4gh.loader.model.es.converters.EsVariantCallPairConverter;
import org.collaboratory.ga4gh.loader.model.es.converters.EsVariantConverter;
import org.collaboratory.ga4gh.loader.model.es.converters.EsVariantSetConverter;
import org.collaboratory.ga4gh.loader.model.metadata.FileMetaDataFetcher;
import org.collaboratory.ga4gh.loader.vcf.CallProcessorManager;
import org.collaboratory.ga4gh.loader.vcf.enums.CallerTypes;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.icgc.dcc.dcc.common.es.DocumentWriterConfiguration;
import org.icgc.dcc.dcc.common.es.core.DocumentWriter;

import lombok.NonNull;
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

  public static DocumentWriter newParentChildDocumentWriter(final Client client) {
    return createDocumentWriter(new DocumentWriterConfiguration()
        .client(client)
        .indexName(PARENT_CHILD_INDEX_NAME)
        .bulkSizeMb(BULK_SIZE_MB)
        .threadsNum(BULK_NUM_THREADS));
  }

  public static DocumentWriter newNestedDocumentWriter(final Client client) {
    return createDocumentWriter(new DocumentWriterConfiguration()
        .client(client)
        .indexName(NESTED_INDEX_NAME)
        .bulkSizeMb(BULK_SIZE_MB)
        .threadsNum(BULK_NUM_THREADS));
  }

  public static IdCacheFactory newIdCacheFactory(boolean useMapDB) {
    final int defaultInitId = 1;
    val defaultStorageDirname = "target";
    if (useMapDB) {
      // return new IdDiskCacheFactory(defaultStorageDirname, defaultInitId);
      return new IdMixedCacheFactory(defaultInitId, defaultStorageDirname);
    } else {
      return new IdRamCacheFactory(defaultInitId);
    }
  }

  public static IdCacheFactory newIdCacheFactory() {
    return newIdCacheFactory(USE_MAP_DB);
  }

  public static CallProcessorManager createCallProcessManager() {
    val allCallerTypesExceptConsensus = newArrayList(CallerTypes.values());
    allCallerTypesExceptConsensus.remove(CallerTypes.consensus);
    return newCallProcessorManager()
        .addCallProcessor(newDummyCallProcessor(), CallerTypes.consensus)
        .addCallProcessor(newUnFilteredBasicCallProcessor(), allCallerTypesExceptConsensus);
  }

  public static EsVariantCallPairConverter newVariantCallPairConverter() {
    val varConv = new EsVariantConverter();
    val callConv = new EsCallConverter();
    return EsVariantCallPairConverter.builder()
        .variantSearchHitConverter(varConv)
        .variantObjectNodeConverter(varConv)
        .callSearchHitConverter(callConv)
        .callObjectNodeConverter(callConv)
        .build();
  }

  public static ParentChild2NestedIndexConverter newParentChild2NestedIndexConverter(@NonNull Client client,
      @NonNull DocumentWriter writer) {
    val converter = newVariantCallPairConverter();
    return ParentChild2NestedIndexConverter.builder()
        .childTypeName(Indexer.CALL_TYPE_NAME)
        .parentTypeName(Indexer.VARIANT_TYPE_NAME)
        .targetTypeName(ParentChild2NestedIndexConverter.VARIANT_NESTED_TYPE_NAME)
        .nestedObjectNodeConverter(converter)
        .searchHitConverter(converter)
        .scrollSize(NESTED_SCROLL_SIZE)
        .writer(writer)
        .client(client)
        .sourceIndexName(PARENT_CHILD_INDEX_NAME)
        .targetIndexName(NESTED_INDEX_NAME)
        .build();
  }

  public static IndexCreatorContext newIndexCreatorContext(@NonNull final Client client, final String indexName){
    return IndexCreatorContext.builder()
            .client(client)
            .indexingEnabled(true)
            .indexName(indexName)
            .indexSettingsFilename(Indexer.INDEX_SETTINGS_JSON_FILENAME)
            .mappingDirname(Indexer.DEFAULT_MAPPINGS_DIRNAME)
            .mappingFilenameExtension(Indexer.DEFAULT_MAPPING_JSON_EXTENSION)
            .typeName(Indexer.CALLSET_TYPE_NAME)
            .typeName(Indexer.VARIANT_SET_TYPE_NAME)
            .typeName(Indexer.VARIANT_TYPE_NAME)
            .typeName(Indexer.VCF_HEADER_TYPE_NAME)
            .typeName(Indexer.CALL_TYPE_NAME)
            .build();
  }

  public static Indexer newIndexer(Client client, DocumentWriter writer,
      IdCacheFactory idCacheFactory)
      throws Exception {
    val indexCreatorContext = newIndexCreatorContext(client, PARENT_CHILD_INDEX_NAME);
    return new Indexer(client, writer, indexCreatorContext,
        idCacheFactory.getVariantIdCache(),
        idCacheFactory.getVariantSetIdCache(),
        idCacheFactory.getCallSetIdCache(),
        new EsVariantSetConverter(),
        new EsCallSetConverter());
  }

  public static Loader newLoader(Client client, DocumentWriter writer, IdCacheFactory idCacheFactory)
      throws Exception {
    val indexer = newIndexer(client, writer, idCacheFactory);
    val callProcessorManager = createCallProcessManager();
    return new Loader(indexer, newStorage(), callProcessorManager);
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
        .storageFilename(DEFAULT_FILE_META_DATA_STORE_FILENAME)
        .somaticSSMsOnly(DATA_FETCHER_SOMATIC_SSMS_ONLY)
        .maxFileSizeBytes(DATA_FETCHER_MAX_FILESIZE_BYTES)
        .limit(DATA_FETCHER_LIMIT)
        .build();
  }

}
