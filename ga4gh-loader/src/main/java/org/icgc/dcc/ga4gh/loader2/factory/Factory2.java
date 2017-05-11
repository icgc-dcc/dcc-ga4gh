package org.icgc.dcc.ga4gh.loader2.factory;

import lombok.NoArgsConstructor;
import lombok.val;
import org.elasticsearch.client.Client;
import org.icgc.dcc.dcc.common.es.DocumentWriterConfiguration;
import org.icgc.dcc.dcc.common.es.DocumentWriterFactory;
import org.icgc.dcc.dcc.common.es.core.DocumentWriter;
import org.icgc.dcc.ga4gh.common.ObjectNodeConverter;
import org.icgc.dcc.ga4gh.common.model.converters.EsCallConverterJson;
import org.icgc.dcc.ga4gh.common.model.converters.EsCallSetConverterJson;
import org.icgc.dcc.ga4gh.common.model.converters.EsVariantCallPairConverterJson2;
import org.icgc.dcc.ga4gh.common.model.converters.EsVariantConverterJson;
import org.icgc.dcc.ga4gh.common.model.converters.EsVariantConverterJson2;
import org.icgc.dcc.ga4gh.common.model.converters.EsVariantSetConverterJson;
import org.icgc.dcc.ga4gh.common.model.es.EsCall;
import org.icgc.dcc.ga4gh.common.model.es.EsCall.EsCallSerializer;
import org.icgc.dcc.ga4gh.common.model.es.EsCallSet.EsCallSetSerializer;
import org.icgc.dcc.ga4gh.common.model.es.EsVariant;
import org.icgc.dcc.ga4gh.common.model.es.EsVariantCallPair2.EsVariantCallPairSerializer;
import org.icgc.dcc.ga4gh.common.model.es.EsVariantSet.EsVariantSetSerializer;
import org.icgc.dcc.ga4gh.loader.indexing.IndexCreatorContext;
import org.icgc.dcc.ga4gh.loader.indexing.Indexer;
import org.icgc.dcc.ga4gh.loader2.Indexer2;
import org.icgc.dcc.ga4gh.loader2.callconverter.CallConverterStrategyMux;
import org.icgc.dcc.ga4gh.loader2.dao.portal.PortalMetadataDao;
import org.icgc.dcc.ga4gh.loader2.dao.portal.PortalMetadataDaoFactory;
import org.icgc.dcc.ga4gh.loader2.persistance.FileObjectRestorerFactory;
import org.icgc.dcc.ga4gh.loader2.portal.Portal;
import org.icgc.dcc.ga4gh.loader2.storage.StorageFactory;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.context.impl.IdStorageContextImpl.IdStorageContextImplSerializer;
import org.mapdb.Serializer;

import java.nio.file.Paths;

import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.ga4gh.common.TypeNames.CALL_SET;
import static org.icgc.dcc.ga4gh.common.TypeNames.VARIANT;
import static org.icgc.dcc.ga4gh.common.TypeNames.VARIANT_SET;
import static org.icgc.dcc.ga4gh.common.TypeNames.VCF_HEADER;
import static org.icgc.dcc.ga4gh.loader.Config.BULK_NUM_THREADS;
import static org.icgc.dcc.ga4gh.loader.Config.BULK_SIZE_MB;
import static org.icgc.dcc.ga4gh.loader.Config.INDEX_NAME;
import static org.icgc.dcc.ga4gh.loader.Config.PERSISTED_DIRPATH;
import static org.icgc.dcc.ga4gh.loader.Config.STORAGE_OUTPUT_VCF_STORAGE_DIR;
import static org.icgc.dcc.ga4gh.loader.Config.TOKEN;
import static org.icgc.dcc.ga4gh.loader2.factory.impl.IntegerIdStorageFactory.createIntegerIdStorageFactory;
import static org.icgc.dcc.ga4gh.loader2.factory.impl.LongIdStorageFactory.createLongIdStorageFactory;
import static org.icgc.dcc.ga4gh.loader2.utils.idstorage.context.impl.IdStorageContextImpl.IdStorageContextImplSerializer.createIdStorageContextSerializer;
import static org.mapdb.Serializer.INTEGER;
import static org.mapdb.Serializer.LONG;

@NoArgsConstructor(access = PRIVATE)
public class Factory2 {
  public static final CallConverterStrategyMux CALL_CONVERTER_STRATEGY_MUX = new CallConverterStrategyMux();

  public static final EsVariantConverterJson2 ES_VARIANT_CONVERTER_JSON_2 = new EsVariantConverterJson2();
  public static final EsVariantSetConverterJson ES_VARIANT_SET_CONVERTER_JSON = new EsVariantSetConverterJson();
  public static final EsCallSetConverterJson ES_CALL_SET_CONVERTER_JSON= new EsCallSetConverterJson();
  public static final EsCallConverterJson ES_CALL_CONVERTER_JSON = new EsCallConverterJson();
  public static final EsVariantConverterJson ES_VARIANT_CONVERTER_JSON = new EsVariantConverterJson();
  public static final EsVariantCallPairConverterJson2 ES_VARIANT_CALL_PAIR_CONVERTER_JSON_2 = EsVariantCallPairConverterJson2.builder()
      .callJsonObjectNodeConverter(ES_CALL_CONVERTER_JSON)
      .callSearchHitConverter(ES_CALL_CONVERTER_JSON)
      .variantJsonObjectNodeConverter(ES_VARIANT_CONVERTER_JSON)
      .variantSearchHitConverter(ES_VARIANT_CONVERTER_JSON)
      .build();

  public static final EsCallSetSerializer ES_CALL_SET_SERIALIZER = new EsCallSetSerializer();
  public static final EsVariantSetSerializer ES_VARIANT_SET_SERIALIZER = new EsVariantSetSerializer();

  //TODO: rtisma HACKK should be EsVariantSerializer impl and not EsVariantOldSerializer.
  // only using old so that can properly deserialize that giant 150GB mapdb file
  public static final Serializer<EsVariant> ES_VARIANT_SERIALIZER = new EsVariant.EsVariantOldSerializer();
  public static final EsCallSerializer ES_CALL_SERIALIZER = new EsCallSerializer();

  public static final EsVariantCallPairSerializer ES_VARIANT_CALL_PAIR_SERIALIZER =
      new EsVariantCallPairSerializer(ES_VARIANT_SERIALIZER, ES_CALL_SERIALIZER);

  public static final IdStorageContextImplSerializer<Long,EsCall> ID_STORAGE_CONTEXT_LONG_SERIALIZER =
      createIdStorageContextSerializer( LONG,ES_CALL_SERIALIZER);

  public static final IdStorageContextImplSerializer<Integer, EsCall> ID_STORAGE_CONTEXT_INTEGER_SERIALIZER =
      IdStorageContextImplSerializer.createIdStorageContextSerializer(INTEGER,ES_CALL_SERIALIZER);


  public static Indexer2 buildIndexer2(Client client, DocumentWriter writer, IndexCreatorContext ctx){
    return new Indexer2(client,writer,ctx,ES_VARIANT_SET_CONVERTER_JSON, ES_CALL_SET_CONVERTER_JSON, ES_VARIANT_CALL_PAIR_CONVERTER_JSON_2);
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
        .indexSettingsFilename(Indexer.INDEX_SETTINGS_JSON_FILENAME)
        .mappingDirname(Indexer.DEFAULT_MAPPINGS_DIRNAME)
        .mappingFilenameExtension(Indexer.DEFAULT_MAPPING_JSON_EXTENSION)
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
    return FileObjectRestorerFactory.createFileObjectRestorerFactory(PERSISTED_DIRPATH);
  }

  public static StorageFactory buildStorageFactory(){
    return StorageFactory.builder()
        .bypassMD5Check(false)
        .outputVcfDir(Paths.get(STORAGE_OUTPUT_VCF_STORAGE_DIR))
        .persistVcfDownloads(true)
        .token(TOKEN)
        .build();
  }

  public static IdStorageFactory<Integer> buildIntegerIdStorageFactory(){
    return createIntegerIdStorageFactory(PERSISTED_DIRPATH);
  }

  public static IdStorageFactory<Long> buildLongIdStorageFactory(){
    return createLongIdStorageFactory(PERSISTED_DIRPATH);
  }
}
