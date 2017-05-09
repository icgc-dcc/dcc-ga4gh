package org.icgc.dcc.ga4gh.loader2;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.ga4gh.common.model.es.EsCall;
import org.icgc.dcc.ga4gh.common.model.es.EsCallSet;
import org.icgc.dcc.ga4gh.common.model.es.EsVariantCallPair2;
import org.icgc.dcc.ga4gh.common.model.es.EsVariantSet;
import org.icgc.dcc.ga4gh.common.model.portal.PortalMetadata;
import org.icgc.dcc.ga4gh.common.types.WorkflowTypes;
import org.icgc.dcc.ga4gh.loader.Config;
import org.icgc.dcc.ga4gh.loader.factory.MainFactory;
import org.icgc.dcc.ga4gh.loader.utils.CounterMonitor;
import org.icgc.dcc.ga4gh.loader2.dao.portal.PortalMetadataDao;
import org.icgc.dcc.ga4gh.loader2.factory.Factory2;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.context.IdStorageContext;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.id.IdStorage;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.isNull;
import static org.icgc.dcc.common.core.util.Joiners.NEWLINE;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;
import static org.icgc.dcc.ga4gh.loader.Config.INDEX_NAME;
import static org.icgc.dcc.ga4gh.loader.Config.LOADER_MODE;
import static org.icgc.dcc.ga4gh.loader.Config.PERSISTED_DIRPATH;
import static org.icgc.dcc.ga4gh.loader.Config.USE_MAP_DB;
import static org.icgc.dcc.ga4gh.loader.Config.VARIANT_MAPDB_ALLOCATION;
import static org.icgc.dcc.ga4gh.loader.Config.VARIANT_MAP_DB_FILENAME;
import static org.icgc.dcc.ga4gh.loader2.CallSetDao.createCallSetDao;
import static org.icgc.dcc.ga4gh.loader2.LoaderModes2.AGGREGATE_ONLY;
import static org.icgc.dcc.ga4gh.loader2.LoaderModes2.FULLY_LOAD;
import static org.icgc.dcc.ga4gh.loader2.LoaderModes2.INDEX_ONLY_BASIC;
import static org.icgc.dcc.ga4gh.loader2.LoaderModes2.INDEX_ONLY_SPECIAL;
import static org.icgc.dcc.ga4gh.loader2.PreProcessor.createPreProcessor;
import static org.icgc.dcc.ga4gh.loader2.VcfProcessor.createVcfProcessor;
import static org.icgc.dcc.ga4gh.loader2.factory.Factory2.ES_CALL_SET_CONVERTER_JSON;
import static org.icgc.dcc.ga4gh.loader2.factory.Factory2.ES_VARIANT_CALL_PAIR_CONVERTER_JSON_2;
import static org.icgc.dcc.ga4gh.loader2.factory.Factory2.ES_VARIANT_SERIALIZER;
import static org.icgc.dcc.ga4gh.loader2.factory.Factory2.ES_VARIANT_SET_CONVERTER_JSON;
import static org.icgc.dcc.ga4gh.loader2.factory.Factory2.buildDefaultPortalMetadataDaoFactory;
import static org.icgc.dcc.ga4gh.loader2.factory.Factory2.buildDocumentWriter;
import static org.icgc.dcc.ga4gh.loader2.factory.Factory2.buildIndexer2;
import static org.icgc.dcc.ga4gh.loader2.factory.Factory2.createDocumentWriter;
import static org.icgc.dcc.ga4gh.loader2.factory.IdStorageFactory.ID_STORAGE_CONTEXT_LONG_SERIALIZER;
import static org.icgc.dcc.ga4gh.loader2.portal.PortalCollabVcfFileQueryCreator.createPortalCollabVcfFileQueryCreator;
import static org.icgc.dcc.ga4gh.loader2.utils.LongCounter2.createLongCounter2;
import static org.icgc.dcc.ga4gh.loader2.utils.idstorage.id.impl.VariantIdStorage.createVariantIdStorage;
import static org.icgc.dcc.ga4gh.loader2.utils.idstorage.storage.MapStorageFactory.createMapStorageFactory;

@Slf4j
public class SecondLoader {

  private static boolean skipPortatMetadata(PortalMetadata portalMetadata){
    val workflowType = WorkflowTypes.parseMatch(portalMetadata.getPortalFilename().getWorkflow(), false);
    val out = workflowType == WorkflowTypes.CONSENSUS || portalMetadata.getFileSize() > 7000000 ;
    return false;
  }

  private static List<PortalMetadata> createAssortedVariantSets(PortalMetadataDao dao, int numSamplesPerWorkflow){
    val maxFileSize = 5000000;
    val map = dao.groupBy(x -> x.getPortalFilename().getWorkflow());
    return map.keySet().stream()
        .flatMap(k -> map.get(k).stream()
            .filter(x -> x.getFileSize() < maxFileSize )
            .limit(numSamplesPerWorkflow))
        .collect(toImmutableList());
  }

  public static void main(String[] args) throws IOException {
    val storage = Factory2.buildStorageFactory().getStorage();
    val localFileRestorerFactory = Factory2.buildFileObjectRestorerFactory();
    val query = createPortalCollabVcfFileQueryCreator();
    val portalMetadataDaoFactory = buildDefaultPortalMetadataDaoFactory(localFileRestorerFactory, query);
    val portalMetadataDao = portalMetadataDaoFactory.getPortalMetadataDao();

    val integerIdStorageFactory = Factory2.buildIntegerIdStorageFactory();
    val longIdStorageFactory = Factory2.buildLongIdStorageFactory();

    val useMapDB = USE_MAP_DB;
    IdStorage<EsVariantSet, Integer> variantSetIdStorage = null ;
    IdStorage<EsCallSet, Integer> callSetIdStorage = null ;
    IdStorage<EsVariantCallPair2, IdStorageContext<Long, EsCall>> variantIdStorage = null ;

    if (Config.LOADER_MODE == FULLY_LOAD || LOADER_MODE == AGGREGATE_ONLY) {

      variantSetIdStorage = integerIdStorageFactory.createVariantSetIdStorage(useMapDB);
      callSetIdStorage = integerIdStorageFactory.createCallSetIdStorage(useMapDB);
      variantIdStorage = longIdStorageFactory.createVariantIdStorage(useMapDB);

      val preProcessor = createPreProcessor(portalMetadataDao, callSetIdStorage, variantSetIdStorage);
      preProcessor.init();

      val callSetDao = createCallSetDao(callSetIdStorage);

      val variantCounterMonitor = CounterMonitor.newMonitor("variantCounterMonitor", 500000);
//      val portalMetadatas =  portalMetadataDao.findAll();
      val portalMetadatas = createAssortedVariantSets(portalMetadataDao, 5);

      long numVariants = 0;
      int count = 0;
      val total = portalMetadatas.size();
      for (val portalMetadata : portalMetadatas) {

        if (skipPortatMetadata(portalMetadata)) {
          continue;
        }

        try {

          log.info("Downloading [{}/{}]: {}", ++count, total, portalMetadata.getPortalFilename().getFilename());
          val vcfFile = storage.getFile(portalMetadata);
          val vcfProcessor = createVcfProcessor(variantIdStorage, variantSetIdStorage, callSetIdStorage,
              callSetDao, variantCounterMonitor);
          variantCounterMonitor.start();
          vcfProcessor.process(portalMetadata, vcfFile);

        } catch (Exception e) {
          log.error("Exception [{}]: {}\n{}", e.getClass().getName(), e.getMessage(), NEWLINE.join(e.getStackTrace()));

        } finally {
          variantCounterMonitor.stop();
          variantCounterMonitor.displaySummary();
        }

      }
      log.info("NumVariants: {}", numVariants);
    }

    if (LOADER_MODE == INDEX_ONLY_BASIC || LOADER_MODE == FULLY_LOAD){
      callSetIdStorage = isNull(callSetIdStorage) ? integerIdStorageFactory.persistCallSetIdStorage() : callSetIdStorage;
      variantSetIdStorage = isNull(variantSetIdStorage) ? integerIdStorageFactory.persistVariantSetIdStorage() : variantSetIdStorage;
      variantIdStorage = isNull(variantIdStorage) ? longIdStorageFactory.createVariantIdStorage(useMapDB) : variantIdStorage;

      try ( val client = MainFactory.newClient();
          val writer = buildDocumentWriter(client)){

        val ctx = Factory2.buildIndexCreatorContext(client);
        val indexer2 = buildIndexer2(client,writer,ctx);
        indexer2.prepareIndex();

        log.info("Indexing VariantSets ...");
        indexer2.indexVariantSets(variantSetIdStorage);

        log.info("Indexing CallSets ...");
        indexer2.indexCallSets(callSetIdStorage);

        log.info("Indexing Variants and Calls...");
        indexer2.indexVariants(variantIdStorage);

        log.info("Indexing COMPLETE");

      } catch (Exception e) {
        log.error("Exception running: ", e);
      }

    } else if (LOADER_MODE == INDEX_ONLY_SPECIAL) {
      Path mapDbPath = PERSISTED_DIRPATH.resolve("variantLongMapStorage.db");
      if (VARIANT_MAP_DB_FILENAME.isPresent()){
        mapDbPath = Paths.get(VARIANT_MAP_DB_FILENAME.get());
      }
      checkState(mapDbPath.toFile().exists() && mapDbPath.toFile().isFile(),
          "the variant map db file [%s] DNE", mapDbPath);
      checkState(mapDbPath.toString().endsWith(".db"), "the variant map db file [%s] does not end with .db");
      val name = mapDbPath.getFileName().toString().replaceAll("\\.db$", "");
      val bulkSizeMb = 100;
      val bulkNumThreads = 5;
      try ( val client = MainFactory.newClient();
          val writer = createDocumentWriter(client, INDEX_NAME, bulkSizeMb, bulkNumThreads)) {
        val ctx = Factory2.buildIndexCreatorContext(client);
        val indexer2 = new Indexer2(client,writer,ctx,ES_VARIANT_SET_CONVERTER_JSON, ES_CALL_SET_CONVERTER_JSON, ES_VARIANT_CALL_PAIR_CONVERTER_JSON_2);
        indexer2.prepareIndex();
        val persistFile = true;
        log.info("Resurrecting map db file [{}] ", mapDbPath);
        val factory = createMapStorageFactory(name, ES_VARIANT_SERIALIZER, ID_STORAGE_CONTEXT_LONG_SERIALIZER,mapDbPath.getParent(),
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
