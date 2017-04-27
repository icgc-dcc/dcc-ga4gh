package org.icgc.dcc.ga4gh.loader2;

import com.google.common.collect.Lists;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.assertj.core.util.Maps;
import org.icgc.dcc.ga4gh.common.model.converters.EsVariantConverterJson2;
import org.icgc.dcc.ga4gh.common.model.es.EsCall;
import org.icgc.dcc.ga4gh.common.model.es.EsVariant2;
import org.icgc.dcc.ga4gh.loader2.persistance.FileObjectRestorerFactory;
import org.icgc.dcc.ga4gh.loader2.storage.StorageFactory;
import org.junit.Ignore;
import org.junit.Test;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;

import java.nio.file.Paths;

import static java.lang.Integer.MAX_VALUE;
import static java.lang.Integer.MIN_VALUE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.ga4gh.loader.Config.STORAGE_OUTPUT_VCF_STORAGE_DIR;
import static org.icgc.dcc.ga4gh.loader.Config.TOKEN;
import static org.icgc.dcc.ga4gh.loader.utils.CounterMonitor.newMonitor;
import static org.icgc.dcc.ga4gh.loader2.dao.portal.PortalMetadataDaoFactory.newDefaultPortalMetadataDaoFactory;
import static org.icgc.dcc.ga4gh.loader2.portal.PortalCollabVcfFileQueryCreator.newPortalCollabVcfFileQueryCreator;
import static org.icgc.dcc.ga4gh.loader2.utils.VCF.newDefaultVCFFileReader;

@Slf4j
public class DaoTest {

  @Test
  @Ignore
  public void testDao(){
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
    int min = MAX_VALUE;
    int max = MIN_VALUE;
    long numVariants = 0;
    val counterMonitor = newMonitor("call", 2000000);
    counterMonitor.start();
    val total = portalMetadataDao.findAll().size();
    int count = 0;
    val variantConverter = new EsVariantConverterJson2();

    for (val portalMetadata : portalMetadataDao.findAll()){
      val file = storage.getFile(portalMetadata);
      log.info("Downloaded [{}/{}]: {}", ++count, total, portalMetadata.getPortalFilename().getFilename());
      for (val variant : newDefaultVCFFileReader(file)){
        val esVariant2 = variantConverter.convertFromVariantContext(variant);
      }
      if (count > 3000){
        break;

      }
    }
    counterMonitor.stop();
    counterMonitor.displaySummary();
    log.info("MinDiff: {}", min);
    log.info("MaxDiff: {}", max);
    log.info("NumVariants: {}", numVariants);

  }

  @Test
  @SneakyThrows
  public void testVariantSerialization(){
    val iVar = EsVariant2.createEsVariant2()
        .setStart(4)
        .setEnd(50)
        .setReferenceBases("GAA")
        .setAlternativeBases(Lists.newArrayList("GAT", "GTT"))
        .setReferenceName("referenceName");

    val variantSerializer = new EsVariant2.EsVariantSerializer();
    val dataOutput2 = new DataOutput2();
    variantSerializer.serialize(dataOutput2, iVar);

    val bytes = dataOutput2.copyBytes();
    val dataInput2 = new DataInput2.ByteArray(bytes);
    val oVar = variantSerializer.deserialize(dataInput2, 0);
    assertThat(iVar).isEqualTo(oVar);
  }

  @Test
  @SneakyThrows
  public void testCallSerialization(){
    val randomMap = Maps.<String, Object>newHashMap();
    randomMap.put("integer", new Integer(9));
    randomMap.put("double", new Double(4.6));
    randomMap.put("string", "string");
    randomMap.put("stringList", Lists.newArrayList("hello", "there"));

    val iCall = EsCall.builder()
        .callSetId(94949)
        .callSetName("sdfsdf")
        .genotypeLikelihood(1.444)
        .info(randomMap)
        .isGenotypePhased(true)
        .nonReferenceAlleles(Lists.newArrayList(1,4,2,5,6))
        .variantSetId(4949)
        .build();

    val callSerializer = new EsCall.EsCallSerializer();
    val dataOutput2 = new DataOutput2();
    callSerializer.serialize(dataOutput2, iCall);

    val bytes = dataOutput2.copyBytes();
    val dataInput2 = new DataInput2.ByteArray(bytes);
    val oCall = callSerializer.deserialize(dataInput2, 0);
    assertThat(iCall).isEqualTo(oCall);
  }

  @Test
  @SneakyThrows
  public void testVariantCallSerialization(){
    val randomMap1 = Maps.<String, Object>newHashMap();
    randomMap1.put("integer", new Integer(9));
    randomMap1.put("double", new Double(4.6));
    randomMap1.put("string", "string");
    randomMap1.put("stringList", Lists.newArrayList("hello", "there"));

    val randomMap2 = Maps.<String, Object>newHashMap();
    randomMap2.put("integer", new Integer(9292));
    randomMap2.put("double", new Double(9393.99));
    randomMap2.put("string", "the string");
    randomMap2.put("integerList", Lists.newArrayList(1,502,9,290));
    randomMap2.put("prevMap", randomMap1);

    val iCall1 = EsCall.builder()
        .callSetId(94949)
        .callSetName("sdfsdf")
        .genotypeLikelihood(1.444)
        .info(randomMap1)
        .isGenotypePhased(true)
        .nonReferenceAlleles(Lists.newArrayList(1,4,2,5,6))
        .variantSetId(4949)
        .build();

    val iCall2 = EsCall.builder()
        .callSetId(9494444)
        .callSetName("sdfsdfe234j")
        .genotypeLikelihood(1.747474)
        .info(randomMap2)
        .isGenotypePhased(false)
        .nonReferenceAlleles(Lists.newArrayList(39,482,99,33))
        .variantSetId(9393)
        .build();

    val iVar = EsVariant2.createEsVariant2()
        .setStart(4)
        .setEnd(50)
        .setReferenceBases("GAA")
        .setAlternativeBases(Lists.newArrayList("GAT", "GTT"))
        .setReferenceName("referenceName")
        .addCall(iCall1)
        .addCall(iCall2);

    val variantSerializer = new EsVariant2.EsVariantSerializer();
    val dataOutput2 = new DataOutput2();
    variantSerializer.serialize(dataOutput2, iVar);

    val bytes = dataOutput2.copyBytes();
    val dataInput2 = new DataInput2.ByteArray(bytes);
    val oVar = variantSerializer.deserialize(dataInput2, 0);
    assertThat(iVar).isEqualTo(oVar);

  }

}
