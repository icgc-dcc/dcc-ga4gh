package org.icgc.dcc.ga4gh.loader.factory;

import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.icgc.dcc.dcc.common.es.DocumentWriterConfiguration;
import org.icgc.dcc.dcc.common.es.DocumentWriterFactory;
import org.icgc.dcc.dcc.common.es.core.DocumentWriter;
import org.icgc.dcc.ga4gh.common.ObjectNodeConverter;
import org.icgc.dcc.ga4gh.common.model.converters.EsCallConverterJson;
import org.icgc.dcc.ga4gh.common.model.converters.EsCallSetConverterJson;
import org.icgc.dcc.ga4gh.common.model.converters.EsVariantCallPairConverterJson;
import org.icgc.dcc.ga4gh.common.model.converters.EsVariantConverterJson;
import org.icgc.dcc.ga4gh.common.model.converters.EsVariantSetConverterJson;
import org.icgc.dcc.ga4gh.common.model.es.EsCall;
import org.icgc.dcc.ga4gh.common.model.es.EsCall.EsCallListSerializer;
import org.icgc.dcc.ga4gh.common.model.es.EsCall.EsCallSerializer;
import org.icgc.dcc.ga4gh.common.model.es.EsCallSet;
import org.icgc.dcc.ga4gh.common.model.es.EsCallSet.EsCallSetSerializer;
import org.icgc.dcc.ga4gh.common.model.es.EsVariant;
import org.icgc.dcc.ga4gh.common.model.es.EsVariantCallPair.EsVariantCallPairSerializer;
import org.icgc.dcc.ga4gh.common.model.es.EsVariantSet;
import org.icgc.dcc.ga4gh.common.model.es.EsVariantSet.EsVariantSetSerializer;
import org.icgc.dcc.ga4gh.loader.Config;
import org.icgc.dcc.ga4gh.loader.callconverter.CallConverterStrategyMux;
import org.icgc.dcc.ga4gh.loader.dao.portal.PortalMetadataDao;
import org.icgc.dcc.ga4gh.loader.dao.portal.PortalMetadataDaoFactory;
import org.icgc.dcc.ga4gh.loader.indexing.IndexCreatorContext;
import org.icgc.dcc.ga4gh.loader.indexing.Indexer;
import org.icgc.dcc.ga4gh.loader.persistance.FileObjectRestorerFactory;
import org.icgc.dcc.ga4gh.loader.portal.Portal;
import org.icgc.dcc.ga4gh.loader.storage.StorageFactory;
import org.icgc.dcc.ga4gh.loader.utils.idstorage.context.IdStorageContext;
import org.icgc.dcc.ga4gh.loader.utils.idstorage.context.impl.IdStorageContextImpl.IdStorageContextImplSerializer;
import org.icgc.dcc.ga4gh.loader.utils.idstorage.id.impl.VariantAggregator;
import org.icgc.dcc.ga4gh.loader.utils.idstorage.storage.MapStorageFactory;
import org.mapdb.Serializer;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;

import static com.google.common.io.Resources.getResource;
import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.ga4gh.common.TypeNames.CALL_SET;
import static org.icgc.dcc.ga4gh.common.TypeNames.VARIANT;
import static org.icgc.dcc.ga4gh.common.TypeNames.VARIANT_SET;
import static org.icgc.dcc.ga4gh.common.TypeNames.VCF_HEADER;
import static org.icgc.dcc.ga4gh.loader.Config.BULK_NUM_THREADS;
import static org.icgc.dcc.ga4gh.loader.Config.BULK_SIZE_MB;
import static org.icgc.dcc.ga4gh.loader.Config.DEFAULT_MAPDB_ALLOCATION;
import static org.icgc.dcc.ga4gh.loader.Config.DEFAULT_MAPPINGS_DIRNAME;
import static org.icgc.dcc.ga4gh.loader.Config.DEFAULT_MAPPING_JSON_EXTENSION;
import static org.icgc.dcc.ga4gh.loader.Config.INDEX_NAME;
import static org.icgc.dcc.ga4gh.loader.Config.INDEX_SETTINGS_JSON_FILENAME;
import static org.icgc.dcc.ga4gh.loader.Config.PERSISTED_DIRPATH;
import static org.icgc.dcc.ga4gh.loader.Config.STORAGE_OUTPUT_VCF_STORAGE_DIR;
import static org.icgc.dcc.ga4gh.loader.Config.TOKEN;
import static org.icgc.dcc.ga4gh.loader.Config.VARIANT_MAPDB_ALLOCATION;
import static org.icgc.dcc.ga4gh.loader.factory.impl.IntegerIdStorageFactory.createIntegerIdStorageFactory;
import static org.icgc.dcc.ga4gh.loader.factory.impl.LongIdStorageFactory.createLongIdStorageFactory;
import static org.icgc.dcc.ga4gh.loader.utils.idstorage.context.impl.IdStorageContextImpl.IdStorageContextImplSerializer.createIdStorageContextSerializer;
import static org.icgc.dcc.ga4gh.loader.utils.idstorage.id.impl.VariantAggregator.createVariantAggregator;
import static org.icgc.dcc.ga4gh.loader.utils.idstorage.storage.MapStorageFactory.createMapStorageFactory;
import static org.mapdb.Serializer.INTEGER;
import static org.mapdb.Serializer.LONG;

@NoArgsConstructor(access = PRIVATE)
public class Factory {
  public static final CallConverterStrategyMux CALL_CONVERTER_STRATEGY_MUX = new CallConverterStrategyMux();

  public static final EsVariantSetConverterJson ES_VARIANT_SET_CONVERTER_JSON = new EsVariantSetConverterJson();
  public static final EsCallSetConverterJson ES_CALL_SET_CONVERTER_JSON= new EsCallSetConverterJson();
  public static final EsCallConverterJson ES_CALL_CONVERTER_JSON = new EsCallConverterJson();
  public static final EsVariantConverterJson ES_VARIANT_CONVERTER_JSON = new EsVariantConverterJson();
  public static final EsVariantCallPairConverterJson ES_VARIANT_CALL_PAIR_CONVERTER_JSON_2 = EsVariantCallPairConverterJson
      .builder()
      .callJsonObjectNodeConverter(ES_CALL_CONVERTER_JSON)
      .callSearchHitConverter(ES_CALL_CONVERTER_JSON)
      .variantJsonObjectNodeConverter(ES_VARIANT_CONVERTER_JSON)
      .variantSearchHitConverter(ES_VARIANT_CONVERTER_JSON)
      .build();

  public static final EsCallSetSerializer ES_CALL_SET_SERIALIZER = new EsCallSetSerializer();
  public static final EsVariantSetSerializer ES_VARIANT_SET_SERIALIZER = new EsVariantSetSerializer();

  //TODO: rtisma HACKK should be EsVariantSerializer impl and not EsVariantOldSerializer.
  // only using old so that can properly deserialize that giant 150GB mapdb file
  public static final Serializer<EsVariant> ES_VARIANT_SERIALIZER = new EsVariant.EsVariantOldSerializer(); //TODO: rtisma_20170511_hack
  public static final EsCallSerializer ES_CALL_SERIALIZER = new EsCallSerializer();

  public static final EsVariantCallPairSerializer ES_VARIANT_CALL_PAIR_SERIALIZER =
      new EsVariantCallPairSerializer(ES_VARIANT_SERIALIZER, ES_CALL_SERIALIZER);

  public static final IdStorageContextImplSerializer<Long,EsCall> ID_STORAGE_CONTEXT_LONG_SERIALIZER =
      createIdStorageContextSerializer( LONG,ES_CALL_SERIALIZER);

  public static final EsCallListSerializer ES_CALL_LIST_SERIALIZER = new EsCallListSerializer(ES_CALL_SERIALIZER);
  public static final IdStorageContextImplSerializer<Integer, EsCall> ID_STORAGE_CONTEXT_INTEGER_SERIALIZER =
      IdStorageContextImplSerializer.createIdStorageContextSerializer(INTEGER,ES_CALL_SERIALIZER);
  private static final String TRANSPORT_SETTINGS_FILENAME =
      "org/icgc/dcc/ga4gh/resources/settings/transport.properties";

  public static Indexer buildIndexer2(Client client, DocumentWriter writer, IndexCreatorContext ctx){
    return new Indexer(client,writer,ctx,ES_VARIANT_SET_CONVERTER_JSON, ES_CALL_SET_CONVERTER_JSON, ES_VARIANT_CALL_PAIR_CONVERTER_JSON_2);
  }

  private static final <ID> String generateMapStorageName(String prefix, Class<ID> type){
    return prefix+type.getSimpleName()+"MapStorage";
  }

  public static DocumentWriter createDocumentWriter(final Client client, String indexName, int bulkSizeMb, int bulkNumThreads) {
    return DocumentWriterFactory.createDocumentWriter(new DocumentWriterConfiguration()
        .client(client)
        .indexName(indexName)
        .bulkSizeMb(bulkSizeMb)
        .threadsNum(bulkNumThreads));
  }

  public static DocumentWriter buildDocumentWriter(Client client){
    return createDocumentWriter(client, INDEX_NAME, BULK_SIZE_MB, BULK_NUM_THREADS);
  }

  public static IndexCreatorContext buildIndexCreatorContext(Client client) {
    return IndexCreatorContext.builder()
        .client(client)
        .indexingEnabled(true)
        .indexName(INDEX_NAME)
        .indexSettingsFilename(INDEX_SETTINGS_JSON_FILENAME)
        .mappingDirname(DEFAULT_MAPPINGS_DIRNAME)
        .mappingFilenameExtension(DEFAULT_MAPPING_JSON_EXTENSION)
        .typeName(CALL_SET)
        .typeName(VARIANT_SET)
        .typeName(VARIANT)
        .typeName(VCF_HEADER)
        .build();

  }

  public static PortalMetadataDaoFactory buildDefaultPortalMetadataDaoFactory(
      FileObjectRestorerFactory fileObjectRestorerFactory, ObjectNodeConverter query) {
    val portal = Portal.builder().jsonQueryGenerator(query).build();
    val persistanceName = PortalMetadataDao.class.getSimpleName();
    return PortalMetadataDaoFactory.createPortalMetadataDaoFactory(persistanceName, portal, fileObjectRestorerFactory);
  }

  public static FileObjectRestorerFactory buildFileObjectRestorerFactory(){
    val persistedPath = RESOURCE_PERSISTED_PATH;
    return FileObjectRestorerFactory.createFileObjectRestorerFactory(persistedPath);
  }

  public static StorageFactory buildStorageFactory(){
    return StorageFactory.builder()
        .bypassMD5Check(false)
        .outputVcfDir(Paths.get(STORAGE_OUTPUT_VCF_STORAGE_DIR))
        .persistVcfDownloads(true)
        .token(TOKEN)
        .build();
  }

  public static final Path RESOURCE_PERSISTED_PATH = PERSISTED_DIRPATH;

  public static final MapStorageFactory<EsVariant, List<EsCall>> VARIANT_AGGREGATOR_MAP_STORAGE_FACTORY= createMapStorageFactory("variantCallListMapStorage",
      ES_VARIANT_SERIALIZER, ES_CALL_LIST_SERIALIZER,
      RESOURCE_PERSISTED_PATH, VARIANT_MAPDB_ALLOCATION );

  public static final MapStorageFactory<EsVariant, IdStorageContext<Long, EsCall>> VARIANT_LONG_MAP_STORAGE_FACTORY= createMapStorageFactory(
      "variantLongMapStorage",
      ES_VARIANT_SERIALIZER, ID_STORAGE_CONTEXT_LONG_SERIALIZER,
      RESOURCE_PERSISTED_PATH, VARIANT_MAPDB_ALLOCATION);

  public static final MapStorageFactory<EsCallSet, Long> CALL_SET_LONG_MAP_STORAGE_FACTORY = createMapStorageFactory(
      "callSetLongMapStorage",
      ES_CALL_SET_SERIALIZER, LONG,
      RESOURCE_PERSISTED_PATH, DEFAULT_MAPDB_ALLOCATION);

  public static final MapStorageFactory<EsVariantSet, Long> VARIANT_SET_LONG_MAP_STORAGE_FACTORY = createMapStorageFactory(
      "variantSetLongMapStorage",
      ES_VARIANT_SET_SERIALIZER, LONG,
      RESOURCE_PERSISTED_PATH, DEFAULT_MAPDB_ALLOCATION);

  public static final MapStorageFactory<EsVariant, IdStorageContext<Integer, EsCall>> VARIANT_INTEGER_MAP_STORAGE_FACTORY= createMapStorageFactory(
      "variantIntegerMapStorage",
      ES_VARIANT_SERIALIZER, ID_STORAGE_CONTEXT_INTEGER_SERIALIZER,
      RESOURCE_PERSISTED_PATH, VARIANT_MAPDB_ALLOCATION);

  public static final MapStorageFactory<EsCallSet, Integer> CALL_SET_INTEGER_MAP_STORAGE_FACTORY = createMapStorageFactory(
      "callSetIntegerMapStorage",
      ES_CALL_SET_SERIALIZER, INTEGER,
      RESOURCE_PERSISTED_PATH, DEFAULT_MAPDB_ALLOCATION);

  public static final MapStorageFactory<EsVariantSet, Integer> VARIANT_SET_INTEGER_MAP_STORAGE_FACTORY = createMapStorageFactory(
      "variantSetIntegerMapStorage",
      ES_VARIANT_SET_SERIALIZER, INTEGER,
      RESOURCE_PERSISTED_PATH, DEFAULT_MAPDB_ALLOCATION);

  public static IdStorageFactory<Integer> buildIntegerIdStorageFactory(){
    return createIntegerIdStorageFactory(VARIANT_INTEGER_MAP_STORAGE_FACTORY, VARIANT_SET_INTEGER_MAP_STORAGE_FACTORY, CALL_SET_INTEGER_MAP_STORAGE_FACTORY);
  }

  public static IdStorageFactory<Long> buildLongIdStorageFactory(){
    return createLongIdStorageFactory(VARIANT_LONG_MAP_STORAGE_FACTORY, VARIANT_SET_LONG_MAP_STORAGE_FACTORY, CALL_SET_LONG_MAP_STORAGE_FACTORY);
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

  public static VariantAggregator buildVariantAggregator(boolean useDisk, boolean persist) {
    val mapStorage = VARIANT_AGGREGATOR_MAP_STORAGE_FACTORY.createMapStorage(useDisk, persist);
    return createVariantAggregator(mapStorage);
  }
}
