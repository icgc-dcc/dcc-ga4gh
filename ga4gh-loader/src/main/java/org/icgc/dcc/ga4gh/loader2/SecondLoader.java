package org.icgc.dcc.ga4gh.loader2;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.ga4gh.common.model.converters.EsVariantConverterJson2;
import org.icgc.dcc.ga4gh.common.model.es.EsCall;
import org.icgc.dcc.ga4gh.common.types.WorkflowTypes;
import org.icgc.dcc.ga4gh.loader2.callconverter.CallConverterStrategyMux;
import org.icgc.dcc.ga4gh.loader2.persistance.FileObjectRestorerFactory;
import org.icgc.dcc.ga4gh.loader2.storage.StorageFactory;

import java.nio.file.Paths;

import static org.icgc.dcc.common.core.util.Joiners.NEWLINE;
import static org.icgc.dcc.ga4gh.common.model.converters.EsVariantSetConverterJson.convertFromPortalMetadata;
import static org.icgc.dcc.ga4gh.common.model.es.EsVcfHeader.createEsVcfHeader;
import static org.icgc.dcc.ga4gh.loader.Config.CALL_SET_ID_STORAGE_DB_PATH;
import static org.icgc.dcc.ga4gh.loader.Config.STORAGE_OUTPUT_VCF_STORAGE_DIR;
import static org.icgc.dcc.ga4gh.loader.Config.TOKEN;
import static org.icgc.dcc.ga4gh.loader.Config.USE_MAP_DB;
import static org.icgc.dcc.ga4gh.loader.Config.VARIANT_SET_ID_STORAGE_DB_PATH;
import static org.icgc.dcc.ga4gh.loader.utils.CounterMonitor.newMonitor;
import static org.icgc.dcc.ga4gh.loader2.CallSetIdStorageFactory.createCallSetIdStorageFactory;
import static org.icgc.dcc.ga4gh.loader2.PreProcessor.createPreProcessor;
import static org.icgc.dcc.ga4gh.loader2.CallSetDao.createCallSetDao;
import static org.icgc.dcc.ga4gh.loader2.VariantSetIdStorageFactory.createVariantSetIdStorageFactory;
import static org.icgc.dcc.ga4gh.loader2.dao.portal.PortalMetadataDaoFactory.newDefaultPortalMetadataDaoFactory;
import static org.icgc.dcc.ga4gh.loader2.portal.PortalCollabVcfFileQueryCreator.newPortalCollabVcfFileQueryCreator;
import static org.icgc.dcc.ga4gh.loader2.utils.VCF.newDefaultVCFFileReader;

@Slf4j
public class SecondLoader {
  private static final boolean F_CHECK_CORRECT_WORKFLOW_TYPE = false;
  private static final CallConverterStrategyMux CALL_CONVERTER_STRATEGY_MUX = new CallConverterStrategyMux();

  private static final EsVariantConverterJson2 ES_VARIANT_CONVERTER_JSON_2 = new EsVariantConverterJson2();

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

    val variantSetIdStorageFactory = createVariantSetIdStorageFactory(VARIANT_SET_ID_STORAGE_DB_PATH, USE_MAP_DB);
    val callSetIdStorageFactory = createCallSetIdStorageFactory(CALL_SET_ID_STORAGE_DB_PATH, USE_MAP_DB);
    val preProcessor = createPreProcessor(portalMetadataDao, variantSetIdStorageFactory, callSetIdStorageFactory);
    preProcessor.init();

    val variantSetIdStorage = preProcessor.getVariantSetIdStorage();
    val callSetIdStorage = preProcessor.getCallSetIdStorage();
    val callSetDao = createCallSetDao(callSetIdStorage);

    long numVariants = 0;
    int count = 0;
    val total = portalMetadataDao.findAll().size();
    val counterMonitor = newMonitor("call", 2000000);
    counterMonitor.start();
    for (val portalMetadata : portalMetadataDao.findAll()){

      try{
        val workflow = portalMetadata.getPortalFilename().getWorkflow();
        val workflowType = WorkflowTypes.parseMatch(workflow, F_CHECK_CORRECT_WORKFLOW_TYPE);
        val file = storage.getFile(portalMetadata);

        val variantSet = convertFromPortalMetadata(portalMetadata);
        val variantSetId = variantSetIdStorage.getId(variantSet);

        val callSet = callSetDao.find(portalMetadata).get();
        val callSetName = callSet.getName();
        val callSetId = callSetIdStorage.getId(callSet);

        log.info("Downloaded [{}/{}]: {}", ++count, total, portalMetadata.getPortalFilename().getFilename());
        val vcfFileReader = newDefaultVCFFileReader(file);
        val vcfFileHeader = vcfFileReader.getFileHeader();
        val esVcfHeader = createEsVcfHeader(portalMetadata, vcfFileHeader);
        val callConverter = CALL_CONVERTER_STRATEGY_MUX.select(portalMetadata);

        val esCallBuilder = EsCall.builder()
            .variantSetId(variantSetId)
            .callSetName(callSetName)
            .callSetId(callSetId);

        for (val variant : vcfFileReader){
          val esCalls = callConverter.convert(esCallBuilder, variant);
          val esVariant2 = ES_VARIANT_CONVERTER_JSON_2.convertFromVariantContext(variant);
          esCalls.forEach(esVariant2::addCall);
        }
      } catch (Exception e){
        log.error("Exception [{}]: {}\n{}", e.getClass().getName(), e.getMessage(), NEWLINE.join(e.getStackTrace()));

      }

    }
    counterMonitor.stop();
    counterMonitor.displaySummary();
    log.info("NumVariants: {}", numVariants);


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
