package org.icgc.dcc.ga4gh.loader;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.ga4gh.common.model.es.EsVariantSet;
import org.icgc.dcc.ga4gh.common.model.portal.PortalMetadata;
import org.icgc.dcc.ga4gh.common.types.WorkflowTypes;
import org.icgc.dcc.ga4gh.loader.factory.Factory;
import org.icgc.dcc.ga4gh.loader.utils.counting.CounterMonitor;
import org.icgc.dcc.ga4gh.loader.utils.idstorage.id.impl.IdStorageFactory2;
import org.icgc.dcc.ga4gh.loader.utils.idstorage.id.impl.IntegerIdStorage;

import java.io.Closeable;
import java.io.IOException;

import static com.google.common.collect.Maps.newHashMap;
import static java.util.Objects.isNull;
import static org.icgc.dcc.common.core.util.Joiners.NEWLINE;
import static org.icgc.dcc.ga4gh.loader.CallSetAccumulator.createCallSetAccumulator;
import static org.icgc.dcc.ga4gh.loader.Config.FILTER_VARIANTS;
import static org.icgc.dcc.ga4gh.loader.VariantFilter.createVariantFilter;
import static org.icgc.dcc.ga4gh.loader.VcfProcessor.createVcfProcessor;
import static org.icgc.dcc.ga4gh.loader.factory.Factory.buildDefaultPortalMetadataDaoFactory;
import static org.icgc.dcc.ga4gh.loader.factory.Factory.buildDocumentWriter;
import static org.icgc.dcc.ga4gh.loader.factory.Factory.buildIndexer2;
import static org.icgc.dcc.ga4gh.loader.portal.PortalConsensusCollabVcfFileQueryCreator.createPortalConsensusCollabVcfFileQueryCreator;
import static org.icgc.dcc.ga4gh.loader.utils.idstorage.storage.impl.RamMapStorage.newRamMapStorage;

@Slf4j
public class Loader {

  private static boolean skipPortatMetadata(PortalMetadata portalMetadata){
    val workflowType = WorkflowTypes.parseMatch(portalMetadata.getPortalFilename().getWorkflow(), false);
    val out = workflowType == WorkflowTypes.CONSENSUS || portalMetadata.getFileSize() > 7000000 ;
    return false;
  }

  public static void main(String[] args) throws IOException {
    val variantFilter = createVariantFilter(!FILTER_VARIANTS);
    val storage = Factory.buildStorageFactory().getStorage();
    val localFileRestorerFactory = Factory.buildFileObjectRestorerFactory();
    val query = createPortalConsensusCollabVcfFileQueryCreator(); //createPortalAllCollabVcfFileQueryCreator();
    val portalMetadataDaoFactory = buildDefaultPortalMetadataDaoFactory(localFileRestorerFactory, query);
    val portalMetadataDao = portalMetadataDaoFactory.getPortalMetadataDao();

    val callSetAccumulator = createCallSetAccumulator(newHashMap(), newHashMap());
    val variantSetIdStorage = IntegerIdStorage.<EsVariantSet>createIntegerIdStorage(newRamMapStorage(),0);

    val variantAggregator = IdStorageFactory2.buildVariantAggregator();

    val variantCounterMonitor = CounterMonitor.createCounterMonitor("variantCounterMonitor", 500000);
    val portalMetadatas =  portalMetadataDao.findAll();

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
        val vcfProcessor = createVcfProcessor(variantAggregator, variantSetIdStorage, callSetAccumulator,
             variantCounterMonitor, variantFilter);
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
    val callSetMapStorage = callSetAccumulator.getMapStorage();

    try (val client = Factory.newClient();
        val writer = buildDocumentWriter(client)) {

      val ctx = Factory.buildIndexCreatorContext(client);
      val indexer2 = buildIndexer2(client, writer, ctx);
      indexer2.prepareIndex();

      log.info("Indexing VariantSets ...");
      indexer2.indexVariantSets(variantSetIdStorage);

      log.info("Indexing CallSets ...");
      indexer2.indexCallSets(callSetMapStorage);

      log.info("Indexing Variants and Calls...");
      val variantIdContextStream = variantAggregator.streamVariantIdContext();
      indexer2.indexVariants(variantIdContextStream);

      log.info("Indexing COMPLETE");

    } catch (Exception e) {
      log.error("Exception running: ", e);
    }

    closeInstance(callSetMapStorage);
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

}
