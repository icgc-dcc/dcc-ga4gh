package org.icgc.dcc.ga4gh.loader2;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.ga4gh.common.types.WorkflowTypes;
import org.icgc.dcc.ga4gh.loader2.persistance.FileObjectRestorerFactory;
import org.icgc.dcc.ga4gh.loader2.storage.StorageFactory;

import java.nio.file.Paths;

import static org.icgc.dcc.common.core.util.Joiners.NEWLINE;
import static org.icgc.dcc.ga4gh.common.model.es.EsVcfHeader.createEsVcfHeader;
import static org.icgc.dcc.ga4gh.loader.Config.CALL_SET_ID_STORAGE_DB_PATH;
import static org.icgc.dcc.ga4gh.loader.Config.STORAGE_OUTPUT_VCF_STORAGE_DIR;
import static org.icgc.dcc.ga4gh.loader.Config.TOKEN;
import static org.icgc.dcc.ga4gh.loader.Config.USE_MAP_DB;
import static org.icgc.dcc.ga4gh.loader.Config.VARIANT_SET_ID_STORAGE_DB_PATH;
import static org.icgc.dcc.ga4gh.loader.utils.CounterMonitor.newMonitor;
import static org.icgc.dcc.ga4gh.loader2.CallSetIdStorageFactory.createCallSetIdStorageFactory;
import static org.icgc.dcc.ga4gh.loader2.PreProcessor.createPreProcessor;
import static org.icgc.dcc.ga4gh.loader2.VariantSetIdStorageFactory.createVariantSetIdStorageFactory;
import static org.icgc.dcc.ga4gh.loader2.dao.portal.PortalMetadataDaoFactory.newDefaultPortalMetadataDaoFactory;
import static org.icgc.dcc.ga4gh.loader2.portal.PortalCollabVcfFileQueryCreator.newPortalCollabVcfFileQueryCreator;
import static org.icgc.dcc.ga4gh.loader2.utils.VCF.newDefaultVCFFileReader;

@Slf4j
public class SecondLoader {
  private static final boolean F_CHECK_CORRECT_WORKFLOW_TYPE = false;

  public static void main(String[] args){
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
    val startStat = new Stat("start");
    val endStat = new Stat("end");
    long numVariants = 0;
    val counterMonitor = newMonitor("call", 2000000);
    counterMonitor.start();
    val total = portalMetadataDao.findAll().size();
    int count = 0;

    val variantSetIdStorageFactory = createVariantSetIdStorageFactory(VARIANT_SET_ID_STORAGE_DB_PATH, USE_MAP_DB);
    val callSetIdStorageFactory = createCallSetIdStorageFactory(CALL_SET_ID_STORAGE_DB_PATH, USE_MAP_DB);
    val preProcessor = createPreProcessor(portalMetadataDao, variantSetIdStorageFactory, callSetIdStorageFactory);
    preProcessor.init();

    for (val portalMetadata : portalMetadataDao.findAll()){
      try{
        val file = storage.getFile(portalMetadata);
        log.info("Downloaded [{}/{}]: {}", ++count, total, portalMetadata.getPortalFilename().getFilename());
        val workflow = portalMetadata.getPortalFilename().getWorkflow();
        val workflowType = WorkflowTypes.parseMatch(workflow, F_CHECK_CORRECT_WORKFLOW_TYPE);
        val vcfFileReader = newDefaultVCFFileReader(file);
        val vcfFileHeader = vcfFileReader.getFileHeader();
        val esVcfHeader = createEsVcfHeader(portalMetadata, vcfFileHeader);
        for (val variant : vcfFileReader){
          numVariants++;
        }
      } catch (Exception e){
        log.error("Exception [{}]: {}\n{}", e.getClass().getName(), e.getMessage(), NEWLINE.join(e.getStackTrace()));

      }

    }
    counterMonitor.stop();
    counterMonitor.displaySummary();
    log.info("NumVariants: {}", numVariants);
    log.info("StartStat: {}", startStat);
    log.info("EndStat: {}", endStat);


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
