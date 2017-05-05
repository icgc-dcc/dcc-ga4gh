package org.icgc.dcc.ga4gh.loader2;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.assertj.core.util.Maps;
import org.icgc.dcc.ga4gh.common.model.converters.EsCallConverterJson;
import org.icgc.dcc.ga4gh.common.model.converters.EsVariantCallPairConverterJson2;
import org.icgc.dcc.ga4gh.common.model.converters.EsVariantConverterJson;
import org.icgc.dcc.ga4gh.common.model.es.EsCall;
import org.icgc.dcc.ga4gh.common.model.es.EsCall.EsCallSerializer;
import org.icgc.dcc.ga4gh.common.model.es.EsVariant;
import org.icgc.dcc.ga4gh.common.model.es.EsVariant.EsVariantSerializer;
import org.icgc.dcc.ga4gh.loader2.persistance.FileObjectRestorerFactory;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.id.impl.IdStorageContext.IdStorageContextSerializer;
import org.junit.Test;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;

import java.nio.file.Path;
import java.nio.file.Paths;

import static com.google.common.collect.Lists.newArrayList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.ga4gh.common.model.es.EsVariantCallPair2.EsVariantCallPairSerializer.createEsVariantCallPairSerializer;
import static org.icgc.dcc.ga4gh.common.model.es.EsVariantCallPair2.createEsVariantCallPair2;
import static org.icgc.dcc.ga4gh.loader.Config.PERSISTED_DIRPATH;
import static org.icgc.dcc.ga4gh.loader.Config.USE_MAP_DB;
import static org.icgc.dcc.ga4gh.loader2.PreProcessor.createPreProcessor;
import static org.icgc.dcc.ga4gh.loader2.dao.portal.PortalMetadataDaoFactory.newDefaultPortalMetadataDaoFactory;
import static org.icgc.dcc.ga4gh.loader2.factory.impl.IntegerIdStorageFactory.createIntegerIdStorageFactory;
import static org.icgc.dcc.ga4gh.loader2.factory.impl.LongIdStorageFactory.createLongIdStorageFactory;
import static org.icgc.dcc.ga4gh.loader2.persistance.FileObjectRestorerFactory.newFileObjectRestorerFactory;
import static org.icgc.dcc.ga4gh.loader2.portal.PortalCollabVcfFileQueryCreator.newPortalCollabVcfFileQueryCreator;
import static org.icgc.dcc.ga4gh.loader2.utils.idstorage.id.impl.IdStorageContext.IdStorageContextSerializer.createIdStorageContextSerializer;

@Slf4j
public class DaoTest {
  private static final Path DEFAULT_PERSISTED_OUTPUT_DIR = Paths.get("test.persisted");
  private static final FileObjectRestorerFactory FILE_OBJECT_RESTORER_FACTORY = newFileObjectRestorerFactory(DEFAULT_PERSISTED_OUTPUT_DIR);
  private static final EsVariantSerializer ES_VARIANT_SERIALIZER = new EsVariantSerializer();
  private static final EsCallSerializer ES_CALL_SERIALIZER = new EsCallSerializer();
  private static final IdStorageContextSerializer<Long,EsCall> ID_STORAGE_CONTEXT_SERIALIZER = createIdStorageContextSerializer(
      Serializer.LONG,ES_CALL_SERIALIZER);

  private int yoyo(int i){
    return i += 4;

  }
  @Test
  public void testMe(){
    int i = 0;
    assertThat(yoyo(i)).isEqualTo(4);
  }

  @Test
  @SneakyThrows
  public void testVariantSerialization(){
    val iVar = EsVariant.builder()
        .start(4)
        .end(50)
        .referenceBases("GAA")
        .alternativeBases(newArrayList("GAT", "GTT"))
        .referenceName("referenceName")
        .build();

    val variantSerializer = new EsVariantSerializer();
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
    randomMap.put("stringList", newArrayList("hello", "there"));

    val iCall = EsCall.builder()
        .callSetId(94949)
        .callSetName("sdfsdf")
        .genotypeLikelihood(1.444)
        .info(randomMap)
        .isGenotypePhased(true)
        .nonReferenceAlleles(newArrayList(1,4,2,5,6))
        .variantSetId(4949)
        .build();

    val callSerializer = new EsCallSerializer();
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
    randomMap1.put("stringList", newArrayList("hello", "there"));

    val randomMap2 = Maps.<String, Object>newHashMap();
    randomMap2.put("integer", new Integer(9292));
    randomMap2.put("double", new Double(9393.99));
    randomMap2.put("string", "the string");
    randomMap2.put("integerList", newArrayList(1,502,9,290));
    randomMap2.put("prevMap", randomMap1);

    val iCall1 = EsCall.builder()
        .callSetId(94949)
        .callSetName("sdfsdf")
        .genotypeLikelihood(1.444)
        .info(randomMap1)
        .isGenotypePhased(true)
        .nonReferenceAlleles(newArrayList(1,4,2,5,6))
        .variantSetId(4949)
        .build();

    val iCall2 = EsCall.builder()
        .callSetId(9494444)
        .callSetName("sdfsdfe234j")
        .genotypeLikelihood(1.747474)
        .info(randomMap2)
        .isGenotypePhased(false)
        .nonReferenceAlleles(newArrayList(39,482,99,33))
        .variantSetId(9393)
        .build();

    val iVar = EsVariant.builder()
        .start(4)
        .end(50)
        .referenceBases("GAA")
        .alternativeBases(newArrayList("GAT", "GTT"))
        .referenceName("referenceName")
        .build();

    val iVariantCallPair = createEsVariantCallPair2(iVar, newArrayList(iCall1,iCall2));

    val variantSerializer = new EsVariantSerializer();
    val callSerializer = new EsCallSerializer();
    val variantCallSerializer = createEsVariantCallPairSerializer(variantSerializer,callSerializer);
    val dataOutput2 = new DataOutput2();
    variantCallSerializer.serialize(dataOutput2, iVariantCallPair);

    val bytes = dataOutput2.copyBytes();
    val dataInput2 = new DataInput2.ByteArray(bytes);
    val oVariantPairPair = variantCallSerializer.deserialize(dataInput2, 0);
    assertThat(iVariantCallPair).isEqualTo(oVariantPairPair);

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
  }

  @Test
  @SneakyThrows
  public void testVariantCallObjectNodeConversion(){
    val randomMap1 = Maps.<String, Object>newHashMap();
    randomMap1.put("integer", new Integer(9));
    randomMap1.put("double", new Double(4.6));
    randomMap1.put("string", "string");
    randomMap1.put("stringList", newArrayList("hello", "there"));

    val randomMap2 = Maps.<String, Object>newHashMap();
    randomMap2.put("integer", new Integer(9292));
    randomMap2.put("double", new Double(9393.99));
    randomMap2.put("string", "the string");
    randomMap2.put("integerList", newArrayList(1,502,9,290));
    randomMap2.put("prevMap", randomMap1);

    val variant = EsVariant.builder()
        .alternativeBase("ACTT")
        .referenceBases("ATCC")
        .referenceName("1")
        .end(10)
        .start(1)
        .build();

    val iCall1 = EsCall.builder()
        .callSetId(94949)
        .callSetName("sdfsdf")
        .genotypeLikelihood(1.444)
        .info(randomMap1)
        .isGenotypePhased(true)
        .nonReferenceAlleles(newArrayList(1,4,2,5,6))
        .variantSetId(4949)
        .build();

    val iCall2 = EsCall.builder()
        .callSetId(9494444)
        .callSetName("sdfsdfe234j")
        .genotypeLikelihood(1.747474)
        .info(randomMap2)
        .isGenotypePhased(false)
        .nonReferenceAlleles(newArrayList(39,482,99,33))
        .variantSetId(9393)
        .build();

    val variantCallPair = createEsVariantCallPair2(variant, newArrayList(iCall1, iCall2));
    val variantConverter = new EsVariantConverterJson();
    val callConverter = new EsCallConverterJson();
    val converter = new EsVariantCallPairConverterJson2(variantConverter, callConverter, variantConverter, callConverter);
    val actualJson = converter.convertToObjectNode(variantCallPair);
    val o = new ObjectMapper();
    val path = Paths.get("src/test/resources/fixtures/variantCallPair.json");
    val expectedJson = o.readTree(path.toFile());

    assertThat(actualJson).isEqualTo(expectedJson);
  }


}
