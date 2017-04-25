package org.icgc.dcc.ga4gh.loader.factory;

import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.ga4gh.common.resources.model.converters.EsCallConverter;
import org.icgc.dcc.ga4gh.common.resources.model.converters.EsCallSetConverter;
import org.icgc.dcc.ga4gh.common.resources.model.converters.EsVariantCallPairConverter;
import org.icgc.dcc.ga4gh.common.resources.model.converters.EsVariantConverter;
import org.icgc.dcc.ga4gh.common.resources.model.converters.EsVariantSetConverter;
import org.icgc.dcc.ga4gh.loader.Loader;
import org.icgc.dcc.ga4gh.loader.Storage;
import org.icgc.dcc.ga4gh.loader.factory.idcache.IdCacheFactory;
import org.icgc.dcc.ga4gh.loader.factory.idcache.impl.IdMixedCacheFactory;
import org.icgc.dcc.ga4gh.loader.factory.idcache.impl.IdRamCacheFactory;
import org.icgc.dcc.ga4gh.loader.indexing.IndexCreatorContext;
import org.icgc.dcc.ga4gh.loader.indexing.Indexer;
import org.icgc.dcc.ga4gh.loader.indexing.ParentChild2NestedIndexConverter;
import org.icgc.dcc.ga4gh.loader.model.metadata.fetcher.Fetcher;
import org.icgc.dcc.ga4gh.loader.vcf.CallProcessorManager;
import org.icgc.dcc.ga4gh.loader.vcf.enums.CallerTypes;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.icgc.dcc.dcc.common.es.DocumentWriterConfiguration;
import org.icgc.dcc.dcc.common.es.core.DocumentWriter;
import org.icgc.dcc.ga4gh.loader.Config;
import org.icgc.dcc.ga4gh.loader.model.metadata.fetcher.decorators.OrderFetcherDecorator;
import org.icgc.dcc.ga4gh.loader.vcf.callprocessors.impl.BasicCallProcessor;
import org.icgc.dcc.ga4gh.loader.vcf.callprocessors.impl.DummyCallProcessor;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Properties;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.io.Resources.getResource;
import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.ga4gh.common.resources.TypeNames.CALL;
import static org.icgc.dcc.ga4gh.common.resources.TypeNames.CALL_SET;
import static org.icgc.dcc.ga4gh.common.resources.TypeNames.VARIANT;
import static org.icgc.dcc.ga4gh.common.resources.TypeNames.VARIANT_NESTED;
import static org.icgc.dcc.ga4gh.common.resources.TypeNames.VARIANT_SET;
import static org.icgc.dcc.ga4gh.common.resources.TypeNames.VCF_HEADER;
import static org.icgc.dcc.dcc.common.es.DocumentWriterFactory.createDocumentWriter;


@Slf4j
@NoArgsConstructor(access = PRIVATE)
public class MainFactory {

  private static final String TRANSPORT_SETTINGS_FILENAME =
      "org/icgc/dcc/ga4gh/resources/settings/transport.properties";

  private static final EsVariantConverter VARIANT_CONVERTER = new EsVariantConverter();
  private static final EsVariantSetConverter VARIANT_SET_CONVERTER = new EsVariantSetConverter();
  private static final EsCallSetConverter CALL_SET_CONVERTER = new EsCallSetConverter();
  private static final EsCallConverter CALL_CONVERTER = new EsCallConverter();

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
        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(Config.NODE_ADDRESS), Config.NODE_PORT));

    return client;
  }

  public static DocumentWriter newParentChildDocumentWriter(final Client client) {
    return createDocumentWriter(new DocumentWriterConfiguration()
        .client(client)
        .indexName(Config.PARENT_CHILD_INDEX_NAME)
        .bulkSizeMb(Config.BULK_SIZE_MB)
        .threadsNum(Config.BULK_NUM_THREADS));
  }

  public static DocumentWriter newNestedDocumentWriter(final Client client) {
    return createDocumentWriter(new DocumentWriterConfiguration()
        .client(client)
        .indexName(Config.NESTED_INDEX_NAME)
        .bulkSizeMb(Config.BULK_SIZE_MB)
        .threadsNum(Config.BULK_NUM_THREADS));
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
    return newIdCacheFactory(Config.USE_MAP_DB);
  }

  public static CallProcessorManager createCallProcessManager() {
    val allCallerTypesExceptConsensus = newArrayList(CallerTypes.values());
    allCallerTypesExceptConsensus.remove(CallerTypes.consensus);
    return CallProcessorManager.newCallProcessorManager()
        .addCallProcessor(DummyCallProcessor.newDummyCallProcessor(), CallerTypes.consensus)
        .addCallProcessor(BasicCallProcessor.newUnFilteredBasicCallProcessor(), allCallerTypesExceptConsensus);
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
        .childTypeName(CALL)
        .parentTypeName(VARIANT)
        .targetTypeName(VARIANT_NESTED)
        .nestedObjectNodeConverter(converter)
        .searchHitConverter(converter)
        .scrollSize(Config.NESTED_SCROLL_SIZE)
        .writer(writer)
        .client(client)
        .sourceIndexName(Config.PARENT_CHILD_INDEX_NAME)
        .targetIndexName(Config.NESTED_INDEX_NAME)
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
            .typeName(CALL_SET)
            .typeName(VARIANT_SET)
            .typeName(VARIANT)
            .typeName(VCF_HEADER)
            .typeName(CALL)
            .build();
  }

  public static Indexer newIndexer(Client client, DocumentWriter writer,
      IdCacheFactory idCacheFactory)
      throws Exception {
    val indexCreatorContext = newIndexCreatorContext(client, Config.PARENT_CHILD_INDEX_NAME);
    return new Indexer(client, writer, indexCreatorContext,
        idCacheFactory.getVariantIdCache(),
        idCacheFactory.getVariantSetIdCache(),
        idCacheFactory.getCallSetIdCache(),
        VARIANT_SET_CONVERTER,
        CALL_SET_CONVERTER,
        VARIANT_CONVERTER,
        CALL_CONVERTER);
  }

  public static Loader newLoader(Client client, DocumentWriter writer, IdCacheFactory idCacheFactory)
      throws Exception {
    val indexer = newIndexer(client, writer, idCacheFactory);
    val callProcessorManager = createCallProcessManager();
    return new Loader(indexer, newStorage(), callProcessorManager, VARIANT_CONVERTER);
  }

  public static Storage newStorage() {
    return new Storage(
        Config.STORAGE_PERSIST_MODE, Config.STORAGE_OUTPUT_VCF_STORAGE_DIR, Config.STORAGE_BYPASS_MD5_CHECK);
  }

  public static Fetcher newFileMetaDataFetcher() {
    val seed = OrderFetcherDecorator.generateSeed();
    log.info("using Seed {} for Fetcher", seed);
    return FetcherFactory.builder()
        .setAllFiles(false)
        .setLimit(Config.DATA_FETCHER_LIMIT > 0, Config.DATA_FETCHER_LIMIT)
        .setSort(Config.SORT_MODE, Config.ASCENDING_MODE)
        .setShuffle(! Config.SORT_MODE, seed)
        .setMaxFileSizeBytes(Config.DATA_FETCHER_MAX_FILESIZE_BYTES>0, Config.DATA_FETCHER_MAX_FILESIZE_BYTES)
        .setSSMFiltering(true) //TODO: need to support for other data types
        .build();
  }
}
