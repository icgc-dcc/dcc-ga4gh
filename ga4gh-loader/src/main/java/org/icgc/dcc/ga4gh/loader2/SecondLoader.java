package org.icgc.dcc.ga4gh.loader2;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.ga4gh.common.model.converters.EsVariantConverterJson2;
import org.icgc.dcc.ga4gh.loader.utils.CounterMonitor;
import org.icgc.dcc.ga4gh.loader2.callconverter.CallConverterStrategyMux;
import org.icgc.dcc.ga4gh.loader2.persistance.FileObjectRestorerFactory;
import org.icgc.dcc.ga4gh.loader2.storage.StorageFactory;

import java.nio.file.Paths;

import static com.google.common.collect.Iterators.partition;
import static org.icgc.dcc.common.core.util.Joiners.NEWLINE;
import static org.icgc.dcc.ga4gh.loader.Config.PERSISTED_DIRPATH;
import static org.icgc.dcc.ga4gh.loader.Config.STORAGE_OUTPUT_VCF_STORAGE_DIR;
import static org.icgc.dcc.ga4gh.loader.Config.TOKEN;
import static org.icgc.dcc.ga4gh.loader.Config.USE_MAP_DB;
import static org.icgc.dcc.ga4gh.loader2.CallSetDao.createCallSetDao;
import static org.icgc.dcc.ga4gh.loader2.PreProcessor.createPreProcessor;
import static org.icgc.dcc.ga4gh.loader2.VcfProcessor.createVcfProcessor;
import static org.icgc.dcc.ga4gh.loader2.dao.portal.PortalMetadataDaoFactory.newDefaultPortalMetadataDaoFactory;
import static org.icgc.dcc.ga4gh.loader2.factory.impl.IntegerIdStorageFactory.createIntegerIdStorageFactory;
import static org.icgc.dcc.ga4gh.loader2.factory.impl.LongIdStorageFactory.createLongIdStorageFactory;
import static org.icgc.dcc.ga4gh.loader2.portal.PortalCollabVcfFileQueryCreator.newPortalCollabVcfFileQueryCreator;

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
