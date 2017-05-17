package org.icgc.dcc.ga4gh.loader;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.ga4gh.common.model.es.EsCall;
import org.icgc.dcc.ga4gh.common.model.es.EsCallSet;
import org.icgc.dcc.ga4gh.common.model.es.EsVariant;
import org.icgc.dcc.ga4gh.common.model.es.EsVariantSet;
import org.icgc.dcc.ga4gh.common.model.portal.PortalMetadata;
import org.icgc.dcc.ga4gh.common.types.WorkflowTypes;
import org.icgc.dcc.ga4gh.loader.dao.portal.PortalMetadataDao;
import org.icgc.dcc.ga4gh.loader.factory.Factory;
import org.icgc.dcc.ga4gh.loader.indexing.Indexer;
import org.icgc.dcc.ga4gh.loader.utils.counting.CounterMonitor;
import org.icgc.dcc.ga4gh.loader.utils.idstorage.context.IdStorageContext;
import org.icgc.dcc.ga4gh.loader.utils.idstorage.id.AbstractIdStorageTemplate;
import org.icgc.dcc.ga4gh.loader.utils.idstorage.id.impl.IdStorageFactory2;
import org.icgc.dcc.ga4gh.loader.utils.idstorage.id.impl.VariantAggregator;
import org.icgc.dcc.ga4gh.loader.utils.idstorage.storage.MapStorage;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.Boolean.TRUE;
import static java.util.Objects.isNull;
import static org.icgc.dcc.common.core.util.Joiners.NEWLINE;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;
import static org.icgc.dcc.ga4gh.loader.CallSetDao.createCallSetDao;
import static org.icgc.dcc.ga4gh.loader.Config.INDEX_NAME;
import static org.icgc.dcc.ga4gh.loader.Config.LOADER_MODE;
import static org.icgc.dcc.ga4gh.loader.Config.USE_MAP_DB;
import static org.icgc.dcc.ga4gh.loader.Config.VARIANT_MAPDB_ALLOCATION;
import static org.icgc.dcc.ga4gh.loader.Config.VARIANT_MAP_DB_FILENAME;
import static org.icgc.dcc.ga4gh.loader.LoaderModes.AGGREGATE_ONLY;
import static org.icgc.dcc.ga4gh.loader.LoaderModes.FULLY_LOAD;
import static org.icgc.dcc.ga4gh.loader.LoaderModes.INDEX_ONLY_BASIC;
import static org.icgc.dcc.ga4gh.loader.LoaderModes.INDEX_ONLY_SPECIAL;
import static org.icgc.dcc.ga4gh.loader.PreProcessor.createPreProcessor;
import static org.icgc.dcc.ga4gh.loader.VcfProcessor.createVcfProcessor;
import static org.icgc.dcc.ga4gh.loader.factory.Factory.ES_CALL_LIST_SERIALIZER;
import static org.icgc.dcc.ga4gh.loader.factory.Factory.ES_CALL_SET_CONVERTER_JSON;
import static org.icgc.dcc.ga4gh.loader.factory.Factory.ES_VARIANT_CALL_PAIR_CONVERTER_JSON_2;
import static org.icgc.dcc.ga4gh.loader.factory.Factory.ES_VARIANT_SERIALIZER;
import static org.icgc.dcc.ga4gh.loader.factory.Factory.ES_VARIANT_SET_CONVERTER_JSON;
import static org.icgc.dcc.ga4gh.loader.factory.Factory.RESOURCE_PERSISTED_PATH;
import static org.icgc.dcc.ga4gh.loader.factory.Factory.buildDefaultPortalMetadataDaoFactory;
import static org.icgc.dcc.ga4gh.loader.factory.Factory.buildDocumentWriter;
import static org.icgc.dcc.ga4gh.loader.factory.Factory.buildIndexer2;
import static org.icgc.dcc.ga4gh.loader.factory.Factory.buildVariantAggregator;
import static org.icgc.dcc.ga4gh.loader.factory.Factory.createDocumentWriter;
import static org.icgc.dcc.ga4gh.loader.portal.PortalCollabVcfFileQueryCreator.createPortalCollabVcfFileQueryCreator;
import static org.icgc.dcc.ga4gh.loader.utils.idstorage.id.impl.VariantAggregator.createVariantAggregator;
import static org.icgc.dcc.ga4gh.loader.utils.idstorage.storage.MapStorageFactory.createMapStorageFactory;

@Slf4j
public class SecondLoader {

  private static boolean skipPortatMetadata(PortalMetadata portalMetadata){
    val workflowType = WorkflowTypes.parseMatch(portalMetadata.getPortalFilename().getWorkflow(), false);
    val out = workflowType == WorkflowTypes.CONSENSUS || portalMetadata.getFileSize() > 7000000 ;
    return false;
  }

  private static List<PortalMetadata> createAssortedVariantSets(PortalMetadataDao dao, int numSamplesPerWorkflow, long maxFileSize){
    val map = dao.groupBy(x -> x.getPortalFilename().getWorkflow());
    return map.keySet().stream()
        .flatMap(k -> map.get(k).stream()
            .filter(x -> x.getFileSize() < maxFileSize )
            .limit(numSamplesPerWorkflow))
        .collect(toImmutableList());
  }

//  @SneakyThrows
//  private static long countLines(File f){
//    val fr = new FileReader(f);
//    val br = new BufferedReader(fr);
//    val iterator = br.lines().iterator();
//    boolean start = false;
//    long count = 0;
//    while(iterator.hasNext()){
//      val line = iterator.next();
//      if (line.startsWith("#CHROM") ){
//        start = true;
//      }
//
//      if (start){
//        ++count;
//      }
//    }
//    return count;
//  }


  public static void main(String[] args) throws IOException {
    val storage = Factory.buildStorageFactory().getStorage();
    val localFileRestorerFactory = Factory.buildFileObjectRestorerFactory();
    val query = createPortalCollabVcfFileQueryCreator();
    val portalMetadataDaoFactory = buildDefaultPortalMetadataDaoFactory(localFileRestorerFactory, query);
    val portalMetadataDao = portalMetadataDaoFactory.getPortalMetadataDao();

    val integerIdStorageFactory = Factory.buildIntegerIdStorageFactory();
    val longIdStorageFactory = Factory.buildLongIdStorageFactory();

    val useMapDB = USE_MAP_DB;
    AbstractIdStorageTemplate<EsVariantSet, Integer> variantSetIdStorage = null ;
    AbstractIdStorageTemplate<EsCallSet, Integer> callSetIdStorage = null ;
    VariantAggregator variantAggregator = null ;
    MapStorage<EsVariant, IdStorageContext<Long, EsCall>> variantMapStorage = null;

    if (Config.LOADER_MODE == FULLY_LOAD || LOADER_MODE == AGGREGATE_ONLY) {

//      variantSetIdStorage = integerIdStorageFactory.createVariantSetIdStorage(useMapDB);
//      callSetIdStorage = integerIdStorageFactory.createCallSetIdStorage(useMapDB);
//      variantAggregator = buildVariantAggregator(useMapDB,FALSE);
      variantSetIdStorage = IdStorageFactory2.buildVariantSetIdStorage();
      callSetIdStorage = IdStorageFactory2.buildCallSetIdStorage();
      variantAggregator = IdStorageFactory2.buildVariantAggregator();

      val preProcessor = createPreProcessor(portalMetadataDao, callSetIdStorage, variantSetIdStorage);
      preProcessor.init();

      val callSetDao = createCallSetDao(callSetIdStorage);

      val variantCounterMonitor = CounterMonitor.createCounterMonitor("variantCounterMonitor", 500000);
      val portalMetadatas =  portalMetadataDao.findAll();

//      val portalMetadatas = createAssortedVariantSets(portalMetadataDao, 2, 80000); //rtisma This is a hack to just load a few files from each variantSet

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
          val vcfProcessor = createVcfProcessor(variantAggregator, variantSetIdStorage, callSetIdStorage,
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
      variantAggregator = isNull(
          variantAggregator) ? buildVariantAggregator(TRUE, TRUE) : variantAggregator;

      try ( val client = Factory.newClient();
          val writer = buildDocumentWriter(client)){

        val ctx = Factory.buildIndexCreatorContext(client);
        val indexer2 = buildIndexer2(client,writer,ctx);
        indexer2.prepareIndex();

        log.info("Indexing VariantSets ...");
        indexer2.indexVariantSets(variantSetIdStorage);

        log.info("Indexing CallSets ...");
        indexer2.indexCallSets(callSetIdStorage);

        log.info("Indexing Variants and Calls...");
        val variantIdContextStream = variantAggregator.streamVariantIdContext();
        indexer2.indexVariants(variantIdContextStream);

        log.info("Indexing COMPLETE");

      } catch (Exception e) {
        log.error("Exception running: ", e);
      }

    } else if (LOADER_MODE == INDEX_ONLY_SPECIAL) {
      val persistedPath = RESOURCE_PERSISTED_PATH;
      Path mapDbPath = persistedPath.resolve("variantLongMapStorage.db");
      if (VARIANT_MAP_DB_FILENAME.isPresent()){
        mapDbPath = Paths.get(VARIANT_MAP_DB_FILENAME.get());
      }
      checkState(mapDbPath.toFile().exists() && mapDbPath.toFile().isFile(),
          "the variant map db file [%s] DNE", mapDbPath);
      checkState(mapDbPath.toString().endsWith(".db"), "the variant map db file [%s] does not end with .db");
      val name = mapDbPath.getFileName().toString().replaceAll("\\.db$", "");
      val bulkSizeMb = 100;
      val bulkNumThreads = 5;
      try ( val client = Factory.newClient();
          val writer = createDocumentWriter(client, INDEX_NAME, bulkSizeMb, bulkNumThreads)) {
        val ctx = Factory.buildIndexCreatorContext(client);
        val indexer2 = new Indexer(client,writer,ctx,ES_VARIANT_SET_CONVERTER_JSON, ES_CALL_SET_CONVERTER_JSON, ES_VARIANT_CALL_PAIR_CONVERTER_JSON_2);
        indexer2.prepareIndex();
        val persistFile = true;
        log.info("Resurrecting map db file [{}] ", mapDbPath);
        val factory = createMapStorageFactory(name, ES_VARIANT_SERIALIZER, ES_CALL_LIST_SERIALIZER,mapDbPath.getParent(),
            VARIANT_MAPDB_ALLOCATION);

        val mapStorage = factory.createDiskMapStorage(TRUE);
        log.info("Creating VariantAggregator object...");
        val variantAggregator2 = createVariantAggregator(mapStorage);

        log.info("Starting indexing of variantAggregator...");
        indexer2.indexVariants(variantAggregator2.streamVariantIdContext());
      } catch (Exception e) {
        log.error("Exception running: ", e);
      }
    }

    closeInstance(callSetIdStorage);
    closeInstance(variantSetIdStorage);
    closeInstance(variantAggregator);
  }

  private static void closeInstance(Closeable closeable){
    if (!isNull(closeable)){
      try {
        closeable.close();
      } catch (Throwable t) {
        log.error("The instance of the class [{}] could not be closed", closeable.getClass().getName());
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
