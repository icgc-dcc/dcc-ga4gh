package org.icgc.dcc.ga4gh.loader2;

import com.google.common.collect.Lists;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.assertj.core.util.Maps;
import org.assertj.core.util.Sets;
import org.icgc.dcc.ga4gh.common.model.converters.EsVariantSetConverterJson;
import org.icgc.dcc.ga4gh.common.model.es.EsCall;
import org.icgc.dcc.ga4gh.common.model.es.EsVariant2;
import org.icgc.dcc.ga4gh.common.model.es.EsVariantSet;
import org.icgc.dcc.ga4gh.loader2.persistance.FileObjectRestorerFactory;
import org.junit.Test;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableSet;
import static org.icgc.dcc.ga4gh.loader.Config.PERSISTED_DIRPATH;
import static org.icgc.dcc.ga4gh.loader.Config.USE_MAP_DB;
import static org.icgc.dcc.ga4gh.loader2.PreProcessor.createPreProcessor;
import static org.icgc.dcc.ga4gh.loader2.dao.portal.PortalMetadataDaoFactory.newDefaultPortalMetadataDaoFactory;
import static org.icgc.dcc.ga4gh.loader2.persistance.FileObjectRestorerFactory.newFileObjectRestorerFactory;
import static org.icgc.dcc.ga4gh.loader2.portal.PortalCollabVcfFileQueryCreator.newPortalCollabVcfFileQueryCreator;
import static org.icgc.dcc.ga4gh.loader2.factory.impl.IntegerIdStorageFactory.createIntegerIdStorageFactory;
import static org.icgc.dcc.ga4gh.loader2.factory.impl.LongIdStorageFactory.createLongIdStorageFactory;

@Slf4j
public class DaoTest {
  private static final Path DEFAULT_PERSISTED_OUTPUT_DIR = Paths.get("test.persisted");
  private static final FileObjectRestorerFactory FILE_OBJECT_RESTORER_FACTORY = newFileObjectRestorerFactory(DEFAULT_PERSISTED_OUTPUT_DIR);

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

  @Test
  public void testPreProcessor(){
    val useDisk = false;
    val variantSetIdPersistPath = Paths.get("variantSetIdStorage.dat");
    val callSetIdPersistPath = Paths.get("callSetIdStorage.dat");

    val query = newPortalCollabVcfFileQueryCreator();
    val portalMetadataDaoFactory = newDefaultPortalMetadataDaoFactory(FILE_OBJECT_RESTORER_FACTORY, query);
    val portalMetadataDao = portalMetadataDaoFactory.getPortalMetadataDao();
    val integerIdStorageFactory = createIntegerIdStorageFactory(PERSISTED_DIRPATH);
    val longIdStorageFactory = createLongIdStorageFactory(PERSISTED_DIRPATH);

    val variantSetIdStorage = integerIdStorageFactory.createVariantSetIdStorage(USE_MAP_DB);
    val callSetIdStorage = integerIdStorageFactory.createCallSetIdStorage(USE_MAP_DB);
    val variantIdStorage = longIdStorageFactory.createVariantIdStorage(USE_MAP_DB);



    val preProcessor = createPreProcessor(portalMetadataDao,callSetIdStorage,variantSetIdStorage);
    preProcessor.init();
    assertThat(preProcessor.isInitialized()).isTrue();
    val variantSetIdMap = preProcessor.getVariantSetIdStorage().getIdMap();
    val callSetIdMap = preProcessor.getCallSetIdStorage().getIdMap();
    val sampleMap = portalMetadataDao.groupBySampleId();


    for (val esCallSet : callSetIdMap.values()){
      val callSetName = esCallSet.getName();
      assertThat(sampleMap).containsKey(callSetName);
      val bioSampleId = esCallSet.getBioSampleId();
      assertThat(callSetName).isEqualTo(bioSampleId);

      val actualSetOfVariantSets = Sets.<EsVariantSet>newHashSet();
      for (val variantSetId : esCallSet.getVariantSetIds()){

        // Check that the variantSetId contained inside esCallSet is actually in the VariantSetIdMap
        assertThat(variantSetIdMap).containsKey(variantSetId);

        val variantSet = variantSetIdMap.get(variantSetId);
        actualSetOfVariantSets.add(variantSet);
      }

      val expectedSetOfVariantSets = sampleMap.get(callSetName).stream()
          .map(EsVariantSetConverterJson::convertFromPortalMetadata)
          .collect(toImmutableSet());

      //Assert that the sets are equal
      assertThat(expectedSetOfVariantSets.containsAll(actualSetOfVariantSets));
      assertThat(actualSetOfVariantSets).containsAll(expectedSetOfVariantSets);
    }
  }

}
