package org.icgc.dcc.ga4gh.loader2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Stopwatch;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.assertj.core.util.Maps;
import org.assertj.core.util.Sets;
import org.icgc.dcc.ga4gh.common.model.converters.EsCallConverterJson;
import org.icgc.dcc.ga4gh.common.model.converters.EsVariantCallPairConverterJson2;
import org.icgc.dcc.ga4gh.common.model.converters.EsVariantConverterJson;
import org.icgc.dcc.ga4gh.common.model.es.EsCall;
import org.icgc.dcc.ga4gh.common.model.es.EsCall.EsCallSerializer;
import org.icgc.dcc.ga4gh.common.model.es.EsVariant;
import org.icgc.dcc.ga4gh.common.model.es.EsVariant.EsVariantSerializer;
import org.icgc.dcc.ga4gh.loader2.persistance.FileObjectRestorerFactory;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.context.impl.IdStorageContextImpl.IdStorageContextImplSerializer;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.context.impl.UIntIdStorageContext;
import org.junit.Test;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static java.lang.Integer.MAX_VALUE;
import static java.lang.Integer.MIN_VALUE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.common.core.util.Formats.formatRate;
import static org.icgc.dcc.ga4gh.common.model.es.EsVariantCallPair2.EsVariantCallPairSerializer.createEsVariantCallPairSerializer;
import static org.icgc.dcc.ga4gh.common.model.es.EsVariantCallPair2.createEsVariantCallPair2;
import static org.icgc.dcc.ga4gh.loader.Config.PERSISTED_DIRPATH;
import static org.icgc.dcc.ga4gh.loader.Config.USE_MAP_DB;
import static org.icgc.dcc.ga4gh.loader2.PreProcessor.createPreProcessor;
import static org.icgc.dcc.ga4gh.loader2.factory.Factory2.buildDefaultPortalMetadataDaoFactory;
import static org.icgc.dcc.ga4gh.loader2.factory.impl.IntegerIdStorageFactory.createIntegerIdStorageFactory;
import static org.icgc.dcc.ga4gh.loader2.factory.impl.LongIdStorageFactory.createLongIdStorageFactory;
import static org.icgc.dcc.ga4gh.loader2.portal.PortalCollabVcfFileQueryCreator.createPortalCollabVcfFileQueryCreator;
import static org.icgc.dcc.ga4gh.loader2.utils.idstorage.context.impl.IdStorageContextImpl.IdStorageContextImplSerializer.createIdStorageContextSerializer;

@Slf4j
public class DaoTest {
  private static final Path DEFAULT_PERSISTED_OUTPUT_DIR = Paths.get("test.persisted");
  private static final FileObjectRestorerFactory FILE_OBJECT_RESTORER_FACTORY = FileObjectRestorerFactory
      .createFileObjectRestorerFactory(DEFAULT_PERSISTED_OUTPUT_DIR);
  private static final EsVariantSerializer ES_VARIANT_SERIALIZER = new EsVariantSerializer();
  private static final EsCallSerializer ES_CALL_SERIALIZER = new EsCallSerializer();
  private static final IdStorageContextImplSerializer<Long,EsCall> ID_STORAGE_CONTEXT_SERIALIZER = createIdStorageContextSerializer(
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

    val query = createPortalCollabVcfFileQueryCreator();
    val portalMetadataDaoFactory = buildDefaultPortalMetadataDaoFactory(FILE_OBJECT_RESTORER_FACTORY, query);
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

  @Test
  public void testK2(){
    char low = Character.MIN_VALUE;
    char high = Character.MAX_VALUE;
    val charSet = Sets.<Character>newHashSet();
    charSet.add('A');
    charSet.add('C');
    charSet.add('G');
    charSet.add('T');
    for (char i=low; i< high; i++){
      val result = isBaseFinalStatic(i);
      assertThat(result).isEqualTo(charSet.contains(i));
    }

  }
  @Test
  public void testK(){
    val n = 99999;
    char low = Character.MIN_VALUE;
    char high = Character.MAX_VALUE;

    long count = n*(long)(high-low);
    val watch1 = Stopwatch.createStarted();
    for (int j =0; j<n;j++){
      for (char i=low; i< high; i++){
        val b = isBaseFinalStatic(i);
      }
    }
    watch1.stop();

    val charSet = Sets.<Character>newHashSet();
    charSet.add('A');
    charSet.add('C');
    charSet.add('G');
    charSet.add('T');

    val watch2 = Stopwatch.createStarted();
    for (int j =0; j<n;j++){
      for (char i=low; i< high; i++){
        val c = isBaseFinalStatic(i);
      }
    }
    watch2.stop();

    log.info("FastWay: {}:  {}", watch1, formatRate(count,watch1));
    log.info("FuncWay:  {}:  {}", watch2, formatRate(count,watch2));


  }


  private final static char f1 = 0x41;
  private final static char mask1 = 0x02;
  private final static char f2 = 0x43;
  private final static char mask2 = 0x04;
  private final static char f3 = 0x54;


  private final static byte com = 0x54;
  private final static byte col1 = 0x47;
  private final static byte col2 = 0x43;


  private final static Map<Byte, Character> DECODE_MAP = Maps.newHashMap();
  private final static Map<Character, Byte> ENCODE_MAP = Maps.newHashMap();
  static {
    DECODE_MAP.put((byte)0x00, 'A');
    DECODE_MAP.put((byte)0x01, 'C');
    DECODE_MAP.put((byte)0x02, 'G');
    DECODE_MAP.put((byte)0x03, 'T');

    ENCODE_MAP.put('A', (byte)0x00 );
    ENCODE_MAP.put('C', (byte)0x01 );
    ENCODE_MAP.put('G', (byte)0x02 );
    ENCODE_MAP.put('T', (byte)0x03 );
  }


  @Test
  public void testEncoderSpeed(){
    val n = 99999;
    char low = Character.MIN_VALUE;
    char high = Character.MAX_VALUE;

    long count = n*(long)(high-low);
    Map<Character, Byte> ENCODE_MAP = Maps.newHashMap();
    val watch1 = Stopwatch.createStarted();
    for (int j =0; j<n;j++){
      for (char i=low; i< high; i++){
        ENCODE_MAP.get(i);
      }
    }
    watch1.stop();

    val watch2 = Stopwatch.createStarted();
    for (int j =0; j<n;j++){
      for (char i=low; i< high; i++){
        val c = encodeBase(i);
      }
    }
    watch2.stop();
    log.info("HashWay: {}:  {}", watch1, formatRate(count,watch1));
    log.info("FastWay: {}:  {}", watch2, formatRate(count,watch2));


  }

  @Test
  public void testDecoderSpeed(){
    val n = 9999999;
    byte low = 0;
    char high = 4;

    long count = n*(long)(high-low);
    Map<Byte, Character> DECODE_MAP = Maps.newHashMap();
    DECODE_MAP.put((byte)0x00, 'A');
    DECODE_MAP.put((byte)0x01, 'C');
    DECODE_MAP.put((byte)0x02, 'G');
    DECODE_MAP.put((byte)0x03, 'T');
    val watch1 = Stopwatch.createStarted();
    for (int j =0; j<n;j++){
      for (byte i=low; i<high; i++){
        DECODE_MAP.get(i);
      }
    }
    watch1.stop();

    val watch2 = Stopwatch.createStarted();
    for (int j =0; j<n;j++){
      for (byte i=low; i< high; i++){
        val c = decodeBase(i);
      }
    }
    watch2.stop();
    log.info("HashWay: {}:  {}", watch1, formatRate(count,watch1));
    log.info("FastWay: {}:  {}", watch2, formatRate(count,watch2));

  }

  private static int encodeBase(char i){
    if (isBaseFinalStatic(i)){
      val c1 = i == col1 | i == com ;
      val c2 = i == col2 | i == com ;
      return (c1 ? 0x02 | (c2 ? 1 : 0) : (c2 ? 1 : 0));
    } else {
      return (byte)0x0F;
    }
  }

  private static final char  decodeBase(byte  i){
    val im = i & 0x3;
    val b0 = ~(im & im >> 1) & 0x1;
    val b1 = ((im ^ im >> 1) & 0x1) << 1;
    val b2 = (im & 0x2) << 1;
    val b3 = 0;
    val b4 = ((im & im >> 1) & 0x1) << 4;
    val b5 = 0x2<<5;
    val out = (char)(b0 | b1 | b2 | b3 | b4 | b5);
    return out;

  }

  @Test
  public void testDecode(){
    assertThat(decodeBase((byte)0x00)).isEqualTo('A');
    assertThat(decodeBase((byte)0x01)).isEqualTo('C');
    assertThat(decodeBase((byte)0x02)).isEqualTo('G');
    assertThat(decodeBase((byte)0x03)).isEqualTo('T');

  }

  @Test
  public void testEncode(){
    assertThat(encodeBase('A')).isEqualTo(0x00);
    assertThat(encodeBase('C')).isEqualTo(0x01);
    assertThat(encodeBase('G')).isEqualTo(0x02);
    assertThat(encodeBase('T')).isEqualTo(0x03);
    assertThat(encodeBase('K')).isEqualTo(0x0F);
  }


  private static boolean isBaseFinalStatic(char x){
    return  (x & ~mask1) == f1 |
        (x & ~mask2) == f2 |
        (x == f3);
  }

  @Test
  public void testUIntIdContext(){
    val ic = UIntIdStorageContext.createUIntIdStorageContext(0);
    assertThat(ic.getId()).isEqualTo(0).as("Zero Test");
    val ic2 = UIntIdStorageContext.createUIntIdStorageContext(MAX_VALUE);
    assertThat(ic2.getId()).isEqualTo(MAX_VALUE).as("Signed Integer Max value Test");
    val ic3 = UIntIdStorageContext.createUIntIdStorageContext((long) MAX_VALUE - MIN_VALUE);
    val iii = ic3.getId();
    assertThat(ic3.getId()).isEqualTo((long) MAX_VALUE - MIN_VALUE).as("Unsigned Integer Max Value test");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testUIntIdContextLT0Error(){
    val ic = UIntIdStorageContext.createUIntIdStorageContext(-1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testUIntIdContextGTMaxError(){
    val ic = UIntIdStorageContext.createUIntIdStorageContext((long)MAX_VALUE - MIN_VALUE + 1L);
  }


}
