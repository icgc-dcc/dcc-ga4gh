package org.icgc.dcc.ga4gh.loader2;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.elasticsearch.client.Client;
import org.icgc.dcc.ga4gh.common.model.converters.EsCallConverterJson;
import org.icgc.dcc.ga4gh.common.model.converters.EsCallSetConverterJson;
import org.icgc.dcc.ga4gh.common.model.converters.EsVariantCallPairConverterJson2;
import org.icgc.dcc.ga4gh.common.model.converters.EsVariantConverterJson;
import org.icgc.dcc.ga4gh.common.model.converters.EsVariantConverterJson2;
import org.icgc.dcc.ga4gh.common.model.converters.EsVariantSetConverterJson;
import org.icgc.dcc.ga4gh.common.model.es.EsCall;
import org.icgc.dcc.ga4gh.common.model.es.EsVariant;
import org.icgc.dcc.ga4gh.common.model.portal.PortalMetadata;
import org.icgc.dcc.ga4gh.common.types.WorkflowTypes;
import org.icgc.dcc.ga4gh.loader.factory.MainFactory;
import org.icgc.dcc.ga4gh.loader.indexing.IndexCreatorContext;
import org.icgc.dcc.ga4gh.loader.indexing.Indexer;
import org.icgc.dcc.ga4gh.loader.utils.CounterMonitor;
import org.icgc.dcc.ga4gh.loader2.callconverter.CallConverterStrategyMux;
import org.icgc.dcc.ga4gh.loader2.persistance.FileObjectRestorerFactory;
import org.icgc.dcc.ga4gh.loader2.storage.StorageFactory;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.id.impl.IdStorageContext;
import org.mapdb.Serializer;

import java.nio.file.Paths;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterators.partition;
import static org.icgc.dcc.common.core.util.Joiners.NEWLINE;
import static org.icgc.dcc.ga4gh.common.TypeNames.CALL_SET;
import static org.icgc.dcc.ga4gh.common.TypeNames.VARIANT;
import static org.icgc.dcc.ga4gh.common.TypeNames.VARIANT_SET;
import static org.icgc.dcc.ga4gh.common.TypeNames.VCF_HEADER;
import static org.icgc.dcc.ga4gh.loader.Config.INDEX_ONLY;
import static org.icgc.dcc.ga4gh.loader.Config.PERSISTED_DIRPATH;
import static org.icgc.dcc.ga4gh.loader.Config.STORAGE_OUTPUT_VCF_STORAGE_DIR;
import static org.icgc.dcc.ga4gh.loader.Config.TOKEN;
import static org.icgc.dcc.ga4gh.loader.Config.USE_MAP_DB;
import static org.icgc.dcc.ga4gh.loader.Config.VARIANT_MAP_DB_FILENAME;
import static org.icgc.dcc.ga4gh.loader.factory.MainFactory.newDocumentWriter;
import static org.icgc.dcc.ga4gh.loader2.CallSetDao.createCallSetDao;
import static org.icgc.dcc.ga4gh.loader2.PreProcessor.createPreProcessor;
import static org.icgc.dcc.ga4gh.loader2.VcfProcessor.createVcfProcessor;
import static org.icgc.dcc.ga4gh.loader2.dao.portal.PortalMetadataDaoFactory.newDefaultPortalMetadataDaoFactory;
import static org.icgc.dcc.ga4gh.loader2.factory.IdStorageFactory.ID_STORAGE_CONTEXT_LONG_SERIALIZER;
import static org.icgc.dcc.ga4gh.loader.Config.VARIANT_MAPDB_ALLOCATION;
import static org.icgc.dcc.ga4gh.loader2.factory.impl.IntegerIdStorageFactory.createIntegerIdStorageFactory;
import static org.icgc.dcc.ga4gh.loader2.factory.impl.LongIdStorageFactory.createLongIdStorageFactory;
import static org.icgc.dcc.ga4gh.loader2.portal.PortalCollabVcfFileQueryCreator.newPortalCollabVcfFileQueryCreator;
import static org.icgc.dcc.ga4gh.loader2.utils.LongCounter2.createLongCounter2;
import static org.icgc.dcc.ga4gh.loader2.utils.idstorage.id.impl.IdStorageContext.IdStorageContextSerializer.createIdStorageContextSerializer;
import static org.icgc.dcc.ga4gh.loader2.utils.idstorage.id.impl.VariantIdStorage.createVariantIdStorage;
import static org.icgc.dcc.ga4gh.loader2.utils.idstorage.storage.MapStorageFactory.createMapStorageFactory;

@Slf4j
public class SecondLoader {
  private static final boolean F_CHECK_CORRECT_WORKFLOW_TYPE = false;
  private static final CallConverterStrategyMux CALL_CONVERTER_STRATEGY_MUX = new CallConverterStrategyMux();

  private static final EsVariantConverterJson2 ES_VARIANT_CONVERTER_JSON_2 = new EsVariantConverterJson2();
  private static final EsVariantSetConverterJson ES_VARIANT_SET_CONVERTER_JSON = new EsVariantSetConverterJson();
  private static final EsCallSetConverterJson ES_CALL_SET_CONVERTER_JSON= new EsCallSetConverterJson();
  private static final EsCallConverterJson ES_CALL_CONVERTER_JSON = new EsCallConverterJson();
  private static final EsVariantConverterJson ES_VARIANT_CONVERTER_JSON = new EsVariantConverterJson();
  private static final EsVariantCallPairConverterJson2 ES_VARIANT_CALL_PAIR_CONVERTER_JSON_2 = EsVariantCallPairConverterJson2.builder()
      .callJsonObjectNodeConverter(ES_CALL_CONVERTER_JSON)
      .callSearchHitConverter(ES_CALL_CONVERTER_JSON)
      .variantJsonObjectNodeConverter(ES_VARIANT_CONVERTER_JSON)
      .variantSearchHitConverter(ES_VARIANT_CONVERTER_JSON)
      .build();

  private static final EsVariant.EsVariantSerializer ES_VARIANT_SERIALIZER = new EsVariant.EsVariantSerializer();
  private static final EsCall.EsCallSerializer ES_CALL_SERIALIZER = new EsCall.EsCallSerializer();
  private static final IdStorageContext.IdStorageContextSerializer<Long,EsCall> ID_STORAGE_CONTEXT_SERIALIZER = createIdStorageContextSerializer(
      Serializer.LONG,ES_CALL_SERIALIZER);
  private static final String TEST_INDEX_NAME = "dcc-variants-test";


  public static void main(String[] args){
    if (!INDEX_ONLY){
      val storage = StorageFactory.builder()
          .bypassMD5Check(false)
          .outputVcfDir(Paths.get(STORAGE_OUTPUT_VCF_STORAGE_DIR))
          .persistVcfDownloads(true)
          .token(TOKEN)
          .build()
          .getStorage();

      val localFileRestorerFactory = FileObjectRestorerFactory.newFileObjectRestorerFactory("test.persisted");
      val query = newPortalCollabVcfFileQueryCreator();
      val portalMetadataDaoFactory = newDefaultPortalMetadataDaoFactory(localFileRestorerFactory, query);
      val portalMetadataDao = portalMetadataDaoFactory.getPortalMetadataDao();
      val integerIdStorageFactory = createIntegerIdStorageFactory(PERSISTED_DIRPATH);
      val longIdStorageFactory = createLongIdStorageFactory(PERSISTED_DIRPATH);

      val useMapDB = USE_MAP_DB;
      val variantSetIdStorage = integerIdStorageFactory.createVariantSetIdStorage(useMapDB);
      val callSetIdStorage = integerIdStorageFactory.createCallSetIdStorage(useMapDB);
      val variantIdStorage = longIdStorageFactory.createVariantIdStorage(useMapDB);

      val preProcessor = createPreProcessor(portalMetadataDao,callSetIdStorage,  variantSetIdStorage);
      preProcessor.init();

      val callSetDao = createCallSetDao(callSetIdStorage);

      long numVariants = 0;
      int count = 0;
      val total = portalMetadataDao.findAll().size();
      val numPartitions = 4;
      val partitions = partition(portalMetadataDao.findAll().iterator(), numPartitions);
      val variantCounterMonitor = CounterMonitor.newMonitor("variantCounterMonitor", 500000);
      for (val portalMetadata : portalMetadataDao.findAll()){

        if (skipPortatMetadata(portalMetadata)){
          continue;
        }
//        if (count > 10){
//          break;
//
//        }

        try{

          log.info("Downloading [{}/{}]: {}", ++count, total, portalMetadata.getPortalFilename().getFilename());
          val vcfFile = storage.getFile(portalMetadata);
          val vcfProcessor = createVcfProcessor( variantIdStorage, variantSetIdStorage, callSetIdStorage,
              callSetDao, variantCounterMonitor);
          variantCounterMonitor.start();
          vcfProcessor.process(portalMetadata, vcfFile);

        } catch (Exception e){
          log.error("Exception [{}]: {}\n{}", e.getClass().getName(), e.getMessage(), NEWLINE.join(e.getStackTrace()));

        } finally{
          variantCounterMonitor.stop();
          variantCounterMonitor.displaySummary();
        }


      }
      log.info("NumVariants: {}", numVariants);
    } else{
      val mapDbPath = Paths.get(VARIANT_MAP_DB_FILENAME);
      checkState(mapDbPath.toFile().exists() && mapDbPath.toFile().isFile(),
          "the variant map db file [%s] DNE", mapDbPath);
      checkState(mapDbPath.toString().endsWith(".db"), "the variant map db file [%s] does not end with .db");
      val name = mapDbPath.getFileName().toString().replaceAll("\\.db$", "");
      val bulkSizeMb = 100;
      val bulkNumThreads = 5;
      try ( val client = MainFactory.newClient();
          val writer = newDocumentWriter(client, TEST_INDEX_NAME, bulkSizeMb, bulkNumThreads)) {
        val ctx = createIndexCreatorContext(client);
        val indexer2 = new Indexer2(client,writer,ctx,ES_VARIANT_SET_CONVERTER_JSON, ES_CALL_SET_CONVERTER_JSON, ES_VARIANT_CALL_PAIR_CONVERTER_JSON_2);
        indexer2.prepareIndex();
        val persistFile = true;
        log.info("Resurrecting map db file [{}] ", mapDbPath);
        val factory = createMapStorageFactory(name,ES_VARIANT_SERIALIZER, ID_STORAGE_CONTEXT_LONG_SERIALIZER,mapDbPath.getParent(),
            VARIANT_MAPDB_ALLOCATION,persistFile);

        val mapStorage = factory.createDiskMapStorage();
        log.info("Creating VariantIdStorage object...");
        val idStorage = createVariantIdStorage(createLongCounter2(0L),mapStorage);

        log.info("Starting indexing of variantIdStorage...");
        indexer2.indexVariants(idStorage);
      } catch (Exception e) {
        log.error("Exception running: ", e);
      }
    }
  }

  private static IndexCreatorContext createIndexCreatorContext(Client client) {
    return IndexCreatorContext.builder()
        .client(client)
        .indexingEnabled(true)
        .indexName(TEST_INDEX_NAME)
        .indexSettingsFilename(Indexer.INDEX_SETTINGS_JSON_FILENAME)
        .mappingDirname(Indexer.DEFAULT_MAPPINGS_DIRNAME)
        .mappingFilenameExtension(Indexer.DEFAULT_MAPPING_JSON_EXTENSION)
        .typeName(CALL_SET)
        .typeName(VARIANT_SET)
        .typeName(VARIANT)
        .typeName(VCF_HEADER)
        .build();

  }

  private static boolean skipPortatMetadata(PortalMetadata portalMetadata){
    val workflowType = WorkflowTypes.parseMatch(portalMetadata.getPortalFilename().getWorkflow(), false);
    val out = workflowType == WorkflowTypes.CONSENSUS || portalMetadata.getFileSize() > 7000000 ;
    return false;
  }

  @RequiredArgsConstructor
  @ToString
  public static class Stat{
    @NonNull private final String name;
    @Getter private int min = Integer.MAX_VALUE;
    @Getter private int max = Integer.MIN_VALUE;

    public void process(int value){
      min = Math.min(min, value);
      max = Math.max(max, value);
    }

  }



}
