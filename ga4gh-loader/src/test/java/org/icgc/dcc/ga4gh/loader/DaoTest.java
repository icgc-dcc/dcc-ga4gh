package org.icgc.dcc.ga4gh.loader;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.assertj.core.util.Maps;
import org.assertj.core.util.Sets;
import org.icgc.dcc.ga4gh.common.model.converters.EsCallConverterJson;
import org.icgc.dcc.ga4gh.common.model.converters.EsVariantCallPairConverterJson;
import org.icgc.dcc.ga4gh.common.model.converters.EsVariantConverterJson;
import org.icgc.dcc.ga4gh.common.model.es.EsCall;
import org.icgc.dcc.ga4gh.common.model.es.EsCall.EsCallSerializer;
import org.icgc.dcc.ga4gh.common.model.es.EsCallSet;
import org.icgc.dcc.ga4gh.common.model.es.EsVariant;
import org.icgc.dcc.ga4gh.common.model.es.EsVariantSet;
import org.icgc.dcc.ga4gh.common.model.portal.PortalFilename;
import org.icgc.dcc.ga4gh.loader.dao.portal.PortalMetadataRequest;
import org.icgc.dcc.ga4gh.loader.factory.Factory;
import org.icgc.dcc.ga4gh.loader.persistance.FileObjectRestorerFactory;
import org.icgc.dcc.ga4gh.loader.storage.impl.LocalStorage;
import org.icgc.dcc.ga4gh.loader.utils.counting.CounterMonitor;
import org.icgc.dcc.ga4gh.loader.utils.counting.LongCounter;
import org.icgc.dcc.ga4gh.loader.utils.idstorage.context.IdStorageContext;
import org.icgc.dcc.ga4gh.loader.utils.idstorage.context.impl.IdStorageContextImpl;
import org.icgc.dcc.ga4gh.loader.utils.idstorage.context.impl.UIntIdStorageContext;
import org.icgc.dcc.ga4gh.loader.utils.idstorage.storage.MapStorage;
import org.icgc.dcc.ga4gh.loader.utils.idstorage.storage.impl.DiskMapStorage;
import org.icgc.dcc.ga4gh.loader.utils.idstorage.storage.impl.RamMapStorage;
import org.junit.Test;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static java.lang.Integer.MAX_VALUE;
import static java.lang.Integer.MIN_VALUE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.common.core.util.Formats.formatRate;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableSet;
import static org.icgc.dcc.ga4gh.common.model.es.EsVariantCallPair.createEsVariantCallPair2;
import static org.icgc.dcc.ga4gh.common.model.portal.PortalFilename.createPortalFilename;
import static org.icgc.dcc.ga4gh.loader.CallSetDao.createCallSetDao;
import static org.icgc.dcc.ga4gh.loader.Config.USE_MAP_DB;
import static org.icgc.dcc.ga4gh.loader.PreProcessor.createPreProcessor;
import static org.icgc.dcc.ga4gh.loader.VcfProcessor.createVcfProcessor;
import static org.icgc.dcc.ga4gh.loader.dao.portal.PortalMetadataRequest.createPortalMetadataRequest;
import static org.icgc.dcc.ga4gh.loader.factory.Factory.ES_CALL_SERIALIZER;
import static org.icgc.dcc.ga4gh.loader.factory.Factory.ES_VARIANT_SERIALIZER;
import static org.icgc.dcc.ga4gh.loader.factory.Factory.RESOURCE_PERSISTED_PATH;
import static org.icgc.dcc.ga4gh.loader.factory.Factory.buildDefaultPortalMetadataDaoFactory;
import static org.icgc.dcc.ga4gh.loader.factory.Factory.buildIntegerIdStorageFactory;
import static org.icgc.dcc.ga4gh.loader.factory.Factory.buildLongIdStorageFactory;
import static org.icgc.dcc.ga4gh.loader.persistance.FileObjectRestorerFactory.createFileObjectRestorerFactory;
import static org.icgc.dcc.ga4gh.loader.portal.PortalCollabVcfFileQueryCreator.createPortalCollabVcfFileQueryCreator;
import static org.icgc.dcc.ga4gh.loader.utils.idstorage.id.impl.IntegerIdStorage.createIntegerIdStorage;
import static org.icgc.dcc.ga4gh.loader.utils.idstorage.id.impl.VariantIdStorage.createVariantIdStorage;

@Slf4j
public class DaoTest {

  private static final Path DEFAULT_PERSISTED_OUTPUT_DIR = Paths.get("test.persisted");
  private static final FileObjectRestorerFactory FILE_OBJECT_RESTORER_FACTORY =
      createFileObjectRestorerFactory(DEFAULT_PERSISTED_OUTPUT_DIR);
  private static final Path TEST_RESOURCES_DIRPATH = Paths.get("src/test/resources");
  private static final Path TEST_FIXTURES_DIRPATH= TEST_RESOURCES_DIRPATH.resolve("fixtures");
  private static final Path TEST_VCF_FILES_DIRPATH= TEST_FIXTURES_DIRPATH.resolve("testVcfFiles");


  @Test
  @SneakyThrows
  public void testVariantSerialization() {
    val iVar = EsVariant.builder()
        .start(4)
        .end(50)
        .referenceBases("GAA")
        .alternativeBases(newArrayList("GAT", "GTT"))
        .referenceName("referenceName")
        .build();

    val variantSerializer = ES_VARIANT_SERIALIZER;
    val dataOutput2 = new DataOutput2();
    variantSerializer.serialize(dataOutput2, iVar);

    val bytes = dataOutput2.copyBytes();
    val dataInput2 = new DataInput2.ByteArray(bytes);
    val oVar = variantSerializer.deserialize(dataInput2, 0);
    assertThat(iVar).isEqualTo(oVar);
  }

  @Test
  @SneakyThrows
  public void testCallSerialization() {
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
        .nonReferenceAlleles(newArrayList(1, 4, 2, 5, 6))
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
  public void testVariantCallSerialization() {
    val randomMap1 = Maps.<String, Object>newHashMap();
    randomMap1.put("integer", new Integer(9));
    randomMap1.put("double", new Double(4.6));
    randomMap1.put("string", "string");
    randomMap1.put("stringList", newArrayList("hello", "there"));

    val randomMap2 = Maps.<String, Object>newHashMap();
    randomMap2.put("integer", new Integer(9292));
    randomMap2.put("double", new Double(9393.99));
    randomMap2.put("string", "the string");
    randomMap2.put("integerList", newArrayList(1, 502, 9, 290));
    randomMap2.put("prevMap", randomMap1);

    val iCall1 = EsCall.builder()
        .callSetId(94949)
        .callSetName("sdfsdf")
        .genotypeLikelihood(1.444)
        .info(randomMap1)
        .isGenotypePhased(true)
        .nonReferenceAlleles(newArrayList(1, 4, 2, 5, 6))
        .variantSetId(4949)
        .build();

    val iCall2 = EsCall.builder()
        .callSetId(9494444)
        .callSetName("sdfsdfe234j")
        .genotypeLikelihood(1.747474)
        .info(randomMap2)
        .isGenotypePhased(false)
        .nonReferenceAlleles(newArrayList(39, 482, 99, 33))
        .variantSetId(9393)
        .build();

    val iVar = EsVariant.builder()
        .start(4)
        .end(50)
        .referenceBases("GAA")
        .alternativeBases(newArrayList("GAT", "GTT"))
        .referenceName("referenceName")
        .build();

    val iVariantCallPair = createEsVariantCallPair2(iVar, newArrayList(iCall1, iCall2));

    val variantSerializer = ES_VARIANT_SERIALIZER;
    val callSerializer = ES_CALL_SERIALIZER;
    val variantCallSerializer = Factory.ES_VARIANT_CALL_PAIR_SERIALIZER;
    val dataOutput2 = new DataOutput2();
    variantCallSerializer.serialize(dataOutput2, iVariantCallPair);

    val bytes = dataOutput2.copyBytes();
    val dataInput2 = new DataInput2.ByteArray(bytes);
    val oVariantPairPair = variantCallSerializer.deserialize(dataInput2, 0);
    assertThat(iVariantCallPair).isEqualTo(oVariantPairPair);

  }

  @Test
  public void testPreProcessor() {
    val useDisk = false;
    val variantSetIdPersistPath = Paths.get("variantSetIdStorage.dat");
    val callSetIdPersistPath = Paths.get("callSetIdStorage.dat");

    val query = createPortalCollabVcfFileQueryCreator();
    val portalMetadataDaoFactory = buildDefaultPortalMetadataDaoFactory(FILE_OBJECT_RESTORER_FACTORY, query);
    val portalMetadataDao = portalMetadataDaoFactory.getPortalMetadataDao();
    val integerIdStorageFactory = buildIntegerIdStorageFactory();
    val longIdStorageFactory = buildLongIdStorageFactory();

    val variantSetIdStorage = integerIdStorageFactory.createVariantSetIdStorage(USE_MAP_DB);
    val callSetIdStorage = integerIdStorageFactory.createCallSetIdStorage(USE_MAP_DB);
    val variantIdStorage = longIdStorageFactory.createVariantIdStorage(USE_MAP_DB);

    val preProcessor = createPreProcessor(portalMetadataDao, callSetIdStorage, variantSetIdStorage);
    preProcessor.init();
    assertThat(preProcessor.isInitialized()).isTrue();
  }

  @Test
  @SneakyThrows
  public void testVariantCallObjectNodeConversion() {
    val randomMap1 = Maps.<String, Object>newHashMap();
    randomMap1.put("integer", new Integer(9));
    randomMap1.put("double", new Double(4.6));
    randomMap1.put("string", "string");
    randomMap1.put("stringList", newArrayList("hello", "there"));

    val randomMap2 = Maps.<String, Object>newHashMap();
    randomMap2.put("integer", new Integer(9292));
    randomMap2.put("double", new Double(9393.99));
    randomMap2.put("string", "the string");
    randomMap2.put("integerList", newArrayList(1, 502, 9, 290));
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
        .nonReferenceAlleles(newArrayList(1, 4, 2, 5, 6))
        .variantSetId(4949)
        .build();

    val iCall2 = EsCall.builder()
        .callSetId(9494444)
        .callSetName("sdfsdfe234j")
        .genotypeLikelihood(1.747474)
        .info(randomMap2)
        .isGenotypePhased(false)
        .nonReferenceAlleles(newArrayList(39, 482, 99, 33))
        .variantSetId(9393)
        .build();

    val variantCallPair = createEsVariantCallPair2(variant, newArrayList(iCall1, iCall2));
    val variantConverter = new EsVariantConverterJson();
    val callConverter = new EsCallConverterJson();
    val converter =
        new EsVariantCallPairConverterJson(variantConverter, callConverter, variantConverter, callConverter);
    val actualJson = converter.convertToObjectNode(variantCallPair);
    val o = new ObjectMapper();
    val path = Paths.get("src/test/resources/fixtures/variantCallPair.json");
    val expectedJson = o.readTree(path.toFile());

    assertThat(actualJson).isEqualTo(expectedJson);
  }

  @Test
  public void testK2() {
    char low = Character.MIN_VALUE;
    char high = Character.MAX_VALUE;
    val charSet = Sets.<Character>newHashSet();
    charSet.add('A');
    charSet.add('C');
    charSet.add('G');
    charSet.add('T');
    for (char i = low; i < high; i++) {
      val result = isBaseFinalStatic(i);
      assertThat(result).isEqualTo(charSet.contains(i));
    }

  }

  @Test
  public void testK() {
    val n = 99999;
    char low = Character.MIN_VALUE;
    char high = Character.MAX_VALUE;

    long count = n * (long) (high - low);
    val watch1 = Stopwatch.createStarted();
    for (int j = 0; j < n; j++) {
      for (char i = low; i < high; i++) {
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
    for (int j = 0; j < n; j++) {
      for (char i = low; i < high; i++) {
        val c = isBaseFinalStatic(i);
      }
    }
    watch2.stop();

    log.info("FastWay: {}:  {}", watch1, formatRate(count, watch1));
    log.info("FuncWay:  {}:  {}", watch2, formatRate(count, watch2));

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
    DECODE_MAP.put((byte) 0x00, 'A');
    DECODE_MAP.put((byte) 0x01, 'C');
    DECODE_MAP.put((byte) 0x02, 'G');
    DECODE_MAP.put((byte) 0x03, 'T');

    ENCODE_MAP.put('A', (byte) 0x00);
    ENCODE_MAP.put('C', (byte) 0x01);
    ENCODE_MAP.put('G', (byte) 0x02);
    ENCODE_MAP.put('T', (byte) 0x03);
  }

  @Test
  public void testEncoderSpeed() {
    val n = 99999;
    char low = Character.MIN_VALUE;
    char high = Character.MAX_VALUE;

    long count = n * (long) (high - low);
    Map<Character, Byte> ENCODE_MAP = Maps.newHashMap();
    val watch1 = Stopwatch.createStarted();
    for (int j = 0; j < n; j++) {
      for (char i = low; i < high; i++) {
        ENCODE_MAP.get(i);
      }
    }
    watch1.stop();

    val watch2 = Stopwatch.createStarted();
    for (int j = 0; j < n; j++) {
      for (char i = low; i < high; i++) {
        val c = encodeBase(i);
      }
    }
    watch2.stop();
    log.info("HashWay: {}:  {}", watch1, formatRate(count, watch1));
    log.info("FastWay: {}:  {}", watch2, formatRate(count, watch2));

  }

  @Test
  public void testDecoderSpeed() {
    val n = 9999999;
    byte low = 0;
    char high = 4;

    long count = n * (long) (high - low);
    Map<Byte, Character> DECODE_MAP = Maps.newHashMap();
    DECODE_MAP.put((byte) 0x00, 'A');
    DECODE_MAP.put((byte) 0x01, 'C');
    DECODE_MAP.put((byte) 0x02, 'G');
    DECODE_MAP.put((byte) 0x03, 'T');
    val watch1 = Stopwatch.createStarted();
    for (int j = 0; j < n; j++) {
      for (byte i = low; i < high; i++) {
        DECODE_MAP.get(i);
      }
    }
    watch1.stop();

    val watch2 = Stopwatch.createStarted();
    for (int j = 0; j < n; j++) {
      for (byte i = low; i < high; i++) {
        val c = decodeBase(i);
      }
    }
    watch2.stop();
    log.info("HashWay: {}:  {}", watch1, formatRate(count, watch1));
    log.info("FastWay: {}:  {}", watch2, formatRate(count, watch2));

  }

  private static int encodeBase(char i) {
    if (isBaseFinalStatic(i)) {
      val c1 = i == col1 | i == com;
      val c2 = i == col2 | i == com;
      return (c1 ? 0x02 | (c2 ? 1 : 0) : (c2 ? 1 : 0));
    } else {
      return (byte) 0x0F;
    }
  }

  private static final char decodeBase(byte i) {
    val im = i & 0x3;
    val b0 = ~(im & im >> 1) & 0x1;
    val b1 = ((im ^ im >> 1) & 0x1) << 1;
    val b2 = (im & 0x2) << 1;
    val b3 = 0;
    val b4 = ((im & im >> 1) & 0x1) << 4;
    val b5 = 0x2 << 5;
    val out = (char) (b0 | b1 | b2 | b3 | b4 | b5);
    return out;

  }

  @Test
  public void testDecode() {
    assertThat(decodeBase((byte) 0x00)).isEqualTo('A');
    assertThat(decodeBase((byte) 0x01)).isEqualTo('C');
    assertThat(decodeBase((byte) 0x02)).isEqualTo('G');
    assertThat(decodeBase((byte) 0x03)).isEqualTo('T');

  }

  @Test
  public void testEncode() {
    assertThat(encodeBase('A')).isEqualTo(0x00);
    assertThat(encodeBase('C')).isEqualTo(0x01);
    assertThat(encodeBase('G')).isEqualTo(0x02);
    assertThat(encodeBase('T')).isEqualTo(0x03);
    assertThat(encodeBase('K')).isEqualTo(0x0F);
  }

  private static boolean isBaseFinalStatic(char x) {
    return (x & ~mask1) == f1 |
        (x & ~mask2) == f2 |
        (x == f3);
  }

  @Test
  public void testUIntIdContext() {
    val ic = UIntIdStorageContext.createUIntIdStorageContext(0);
    assertThat(ic.getId()).isEqualTo(0).as("Zero Test");
    val ic2 = UIntIdStorageContext.createUIntIdStorageContext(MAX_VALUE);
    assertThat(ic2.getId()).isEqualTo(MAX_VALUE).as("Signed Integer Max value Test");
    val ic3 = UIntIdStorageContext.createUIntIdStorageContext((long) MAX_VALUE - MIN_VALUE);
    val iii = ic3.getId();
    assertThat(ic3.getId()).isEqualTo((long) MAX_VALUE - MIN_VALUE).as("Unsigned Integer Max Value test");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testUIntIdContextLT0Error() {
    val ic = UIntIdStorageContext.createUIntIdStorageContext(-1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testUIntIdContextGTMaxError() {
    val ic = UIntIdStorageContext.createUIntIdStorageContext((long) MAX_VALUE - MIN_VALUE + 1L);
  }

  @Test
  @SneakyThrows
  public void testIdStorageSerialization() {
    val ser = new IdStorageContextImpl.IdStorageContextImplSerializer<Long, EsCall>(Serializer.LONG,
        new EsCall.EsCallSerializer());
    val info = Maps.<String, Object>newHashMap();
    info.put("field1", "value1");
    info.put("field2", "value2");
    val alleleList1 = Lists.newArrayList(1, 6, 3, 0);
    val alleleList2 = Lists.newArrayList(-1, 4, 5, 6);
    val alleleList3 = Lists.newArrayList(100, 102, 302);
    val esCall1 = EsCall.builder()
        .callSetId(1)
        .callSetName("one")
        .genotypeLikelihood(1.0)
        .isGenotypePhased(true)
        .variantSetId(100)
        .info(info)
        .nonReferenceAlleles(alleleList1)
        .build();

    val esCall2 = EsCall.builder()
        .callSetId(2)
        .callSetName("two")
        .genotypeLikelihood(2.0)
        .isGenotypePhased(true)
        .variantSetId(200)
        .info(info)
        .nonReferenceAlleles(alleleList2)
        .build();

    val esCall3 = EsCall.builder()
        .callSetId(3)
        .callSetName("three")
        .genotypeLikelihood(3.0)
        .isGenotypePhased(true)
        .variantSetId(300)
        .info(info)
        .nonReferenceAlleles(alleleList3)
        .build();

    val iCtx = IdStorageContextImpl.<Long, EsCall>createIdStorageContext(1L);
    iCtx.add(esCall1);
    iCtx.add(esCall2);
    iCtx.add(esCall3);

    val dataOutput2 = new DataOutput2();
    ser.serialize(dataOutput2, iCtx);

    val bytes = dataOutput2.copyBytes();
    val dataInput2 = new DataInput2.ByteArray(bytes);
    val oCtx = ser.deserialize(dataInput2, 0);
    assertThat(oCtx).isEqualTo(iCtx);
  }

  /**
   * This test proves that when using mapDb (via DiskMapStorage object), when the value of the key-value pair is a LIST,
   * after updating the list of the existing key-value pair, you need to "put" it back in to trigger a commit or write to disk.
   * This can be shown by comparing RamMapStorage and DiskMapStorage, as RamMapStorage will contiain 3 elements in the list
   * however DiskMapStorage will only contain the first element.
   */
  @Test
  @SneakyThrows
  public void testIdStorageContextMapDbHack() {
    val persistedPath = RESOURCE_PERSISTED_PATH;
    val idStorageContextSeriliazer =
        new IdStorageContextImpl.IdStorageContextImplSerializer<Long, EsCall>(Serializer.LONG,
            new EsCall.EsCallSerializer());
    //            val variantMapStorage = RamMapStorage.<EsVariant, IdStorageContext<Long, EsCall>>newRamMapStorage();
    val variantMapStorage =
        DiskMapStorage.<EsVariant, IdStorageContext<Long, EsCall>>newDiskMapStorage("testVariantMapStorage",
            new EsVariant.EsVariantSerializer(), idStorageContextSeriliazer, persistedPath, 0, false);

    val info = Maps.<String, Object>newHashMap();
    info.put("field1", "value1");
    info.put("field2", "value2");
    val alleleList1 = Lists.newArrayList(1, 6, 3, 0);
    val alleleList2 = Lists.newArrayList(-1, 4, 5, 6);
    val alleleList3 = Lists.newArrayList(100, 102, 302);
    val esCall1 = EsCall.builder()
        .callSetId(1)
        .callSetName("one")
        .genotypeLikelihood(1.0)
        .isGenotypePhased(true)
        .variantSetId(100)
        .info(info)
        .nonReferenceAlleles(alleleList1)
        .build();

    val esCall2 = EsCall.builder()
        .callSetId(2)
        .callSetName("two")
        .genotypeLikelihood(2.0)
        .isGenotypePhased(true)
        .variantSetId(200)
        .info(info)
        .nonReferenceAlleles(alleleList2)
        .build();

    val esCall3 = EsCall.builder()
        .callSetId(3)
        .callSetName("three")
        .genotypeLikelihood(3.0)
        .isGenotypePhased(true)
        .variantSetId(300)
        .info(info)
        .nonReferenceAlleles(alleleList3)
        .build();

    val iCtx = IdStorageContextImpl.<Long, EsCall>createIdStorageContext(1L);
    iCtx.add(esCall1);
    iCtx.add(esCall2);
    iCtx.add(esCall3);

    val esVariant = EsVariant.builder()
        .start(755904)
        .referenceName("1")
        .referenceBases("G")
        .alternativeBase("A")
        .end(755904)
        .build();

    val map = variantMapStorage.getMap();
    val inputCtx = IdStorageContextImpl.<Long, EsCall>createIdStorageContext(1L);
    inputCtx.add(esCall1);
    map.put(esVariant, inputCtx);
    val inputCtx2 = map.get(esVariant);
    inputCtx2.add(esCall2);
    inputCtx2.add(esCall3);

    val ctxWithoutPut = map.get(esVariant);
    val setWithoutPut = ctxWithoutPut.getObjects().stream().collect(toImmutableSet());
    assertThat(setWithoutPut).contains(esCall1);
    assertThat(setWithoutPut.contains(esCall2)).isFalse();
    assertThat(setWithoutPut.contains(esCall3)).isFalse();
    assertThat(setWithoutPut).hasSize(1);

    //TODO: rtisma HACK FIX - this issues a commit for DiskMapStorage, but is redundant for RamMapStorage
    map.put(esVariant, inputCtx2);
    val ctxWithPut = map.get(esVariant);
    val setWithPut = ctxWithPut.getObjects().stream().collect(toImmutableSet());
    assertThat(setWithPut).contains(esCall1);
    assertThat(setWithPut).contains(esCall2);
    assertThat(setWithPut).contains(esCall3);
    assertThat(setWithPut).hasSize(3);

  }

  @Test
  @SneakyThrows
  public void testRamIdStorageContext(){
    val variantMapStorage = RamMapStorage.<EsVariant, IdStorageContext<Long, EsCall>>newRamMapStorage();
    runIdStorageContextTest(variantMapStorage);
  }

  @Test
  @SneakyThrows
  public void testDiskIdStorageContext(){
    val persistedPath = RESOURCE_PERSISTED_PATH;
    val idStorageContextSeriliazer = new IdStorageContextImpl.IdStorageContextImplSerializer<Long, EsCall>(Serializer.LONG, new EsCall.EsCallSerializer());
    val variantMapStorage = DiskMapStorage.<EsVariant, IdStorageContext<Long, EsCall>>newDiskMapStorage("testVariantMapStorage", new EsVariant.EsVariantSerializer(), idStorageContextSeriliazer,persistedPath,0,false );
    runIdStorageContextTest(variantMapStorage);
  }

  @SneakyThrows
  private void runIdStorageContextTest(MapStorage<EsVariant, IdStorageContext<Long, EsCall>> variantMapStorage){
    val portalFilenames = Lists.<PortalFilename>newArrayList();
    portalFilenames.add(createPortalFilename("120f01d1-8884-4aca-a1cb-36b207b2aa3a.dkfz-snvCalling_1-0-132-1.20150903.somatic.snv_mnv.vcf.gz"));
    portalFilenames.add(createPortalFilename("145f2b89-8878-4390-b0f6-f09b02fb138a.svcp_1-0-5.20150807.somatic.snv_mnv.vcf.gz"));
    portalFilenames.add(createPortalFilename("32d8c373-b5c8-420b-9808-8812b5501649.dkfz-snvCalling_1-0-132-1.20150820.somatic.snv_mnv.vcf.gz"));

    val fileObjectRestorerFactory = createFileObjectRestorerFactory(RESOURCE_PERSISTED_PATH);
    val portalMetadataDaoFactory = buildDefaultPortalMetadataDaoFactory(fileObjectRestorerFactory, createPortalCollabVcfFileQueryCreator());
    val portalMetadataDao = portalMetadataDaoFactory.getPortalMetadataDao();

//    val storage = createPortalStorage(true, vcfStorageDirpath,false,TOKEN);
    val storage = LocalStorage.newLocalStorage(TEST_VCF_FILES_DIRPATH, true);
    val vcfFiles = portalFilenames.stream()
        .map(PortalMetadataRequest::createPortalMetadataRequest)
        .map(portalMetadataDao::find)
        .flatMap(Collection::stream)
        .map(storage::getFile)
        .collect(toImmutableSet());

    val variantCounter = LongCounter.createLongCounter(0L);
    val variantIdStorage = createVariantIdStorage(variantCounter,variantMapStorage);

    val variantSetMapStorage = RamMapStorage.<EsVariantSet, Integer>newRamMapStorage();
    val variantSetIdStorage = createIntegerIdStorage(variantSetMapStorage, 0);

    val callSetMapStorage = RamMapStorage.<EsCallSet, Integer>newRamMapStorage();
    val callSetIdStorage = createIntegerIdStorage(callSetMapStorage, 0);


    val variantCounterMonitor = CounterMonitor.createCounterMonitor("variant_cm", 1000);


    val preProcessor =  createPreProcessor(portalMetadataDao,callSetIdStorage,variantSetIdStorage);
    preProcessor.init();
    val callSetDao = createCallSetDao(callSetIdStorage);

    val vcfProcessor = createVcfProcessor(variantIdStorage,variantSetIdStorage,callSetIdStorage,callSetDao, variantCounterMonitor);


    for (val vcfFile : vcfFiles){
      val vcfPath = vcfFile.toPath();
      if (vcfPath.getFileName().toString().endsWith(".vcf.gz")){
        val request = createPortalMetadataRequest(createPortalFilename(vcfPath.getFileName().toString()));
        val result = portalMetadataDao.find(request);
        assertThat(result).hasSize(1);
        val portalMetadata = result.get(0);
        vcfProcessor.process(portalMetadata, vcfPath.toFile());
      }
    }

    //Search for below variant, there should be 3 calls for this variant given the 3 files above
    //1    755904 .       G     A       .       LOWSUPPORT;NORMALPANEL
    val esVariant = EsVariant.builder()
        .start(755904)
        .referenceName("1")
        .referenceBases("G")
        .alternativeBase("A")
        .end(755904)
        .build();


    val ctx = variantMapStorage.getMap().get(esVariant);

    log.info("Variant: {}", esVariant);
    log.info("IdStorageContext: {}", ctx);

    val set = ctx.getObjects().stream().collect(toImmutableSet());
    assertThat(set).hasSize(3);
  }


  @Test
  @SneakyThrows
  public void testMapDbSanity() {
    val persistedPath = RESOURCE_PERSISTED_PATH;
    val mapStorage = DiskMapStorage.<Long, String>newDiskMapStorage("testMapdb", Serializer.LONG,
        Serializer.STRING,persistedPath,0, false);

    val map = mapStorage.getMap();
    map.put(1L, "first");
    map.put(2L, "second");
    map.put(3L, "third");

    assertThat(map.keySet()).hasSize(3);
    assertThat(map.get(1L)).isEqualTo("first");
    assertThat(map.get(2L)).isEqualTo("second");
    assertThat(map.get(3L)).isEqualTo("third");

  }
}
