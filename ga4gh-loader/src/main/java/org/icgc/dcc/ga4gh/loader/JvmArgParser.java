package org.icgc.dcc.ga4gh.loader;

import lombok.Builder;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.val;

import java.lang.management.ManagementFactory;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;

@RequiredArgsConstructor
public class JvmArgParser {

  private static final String MIN_HEAP_ARG_PREFIX = "-Xms";
  private static final String MAX_HEAP_ARG_PREFIX = "-Xmx";
  private static final String DIRECT_MEM_SIZE_ARG_PREFIX = "-XX:MaxDirectMemorySize=";

  @NonNull private final List<String> jvmArgs;

  public JvmArgs parse(){
    return JvmArgs.builder()
        .directMemorySize(parseDirectMemSize())
        .maxHeapSizeGb(parseMaxHeapSize())
        .minHeapSizeGb(parseMinHeapSize())
        .build();
  }

  private int parseDirectMemSize(){
    val cap = parseSuffix(DIRECT_MEM_SIZE_ARG_PREFIX).toLowerCase();
    checkState(cap.endsWith("g"), "The Min Heap size value [%s] does not end with [g]", cap);
    val temp = cap.replaceAll("g$", "");
    return parseInt(temp);
  }

  private String parseSuffix(String key){
    val opt = jvmArgs.stream()
        .filter(x -> x.startsWith(key))
        .map(x -> x.replaceAll("^"+key, ""))
        .findFirst();
    return opt.orElseThrow(() -> new IllegalStateException(format("There is not jvm argument that starts with the key [%s] ", key)));
  }

  private int parseIntSwitch(String prefix){
    val cap = parseSuffix(prefix).toLowerCase();
    checkState(cap.endsWith("g"), "The IntSwitch [%s] does not end with [g] or [G]", cap);
    val temp = cap.replaceAll("g$", "");
    return parseInt(temp);
  }

  private int parseMinHeapSize(){
    return parseIntSwitch(MIN_HEAP_ARG_PREFIX);
  }

  private int parseMaxHeapSize(){
    return parseIntSwitch(MAX_HEAP_ARG_PREFIX);
  }

  public static JvmArgParser createJvmArgParser() {
    val argList = ManagementFactory.getRuntimeMXBean().getInputArguments();
    return createJvmArgParser(argList);
  }

  public static JvmArgParser createJvmArgParser(List<String> jvmArgs) {
    return new JvmArgParser(jvmArgs);
  }

  @Value
  @Builder
  public static class JvmArgs{

    private static final long BYTES_PER_CALL = 650; // Based off of past OutOfMemory errors
    private static final int MIN_HEAP_SIZE_GB = 2;

    private final long minHeapSizeGb;
    private final long maxHeapSizeGb;
    private final long directMemorySize;

    public void check(long numCalls){
      checkHeapMinSize();
      checkDirectMemoryMinReq(numCalls);
    }

    private void checkHeapMinSize(){
      checkArgument(minHeapSizeGb >= MIN_HEAP_SIZE_GB, "The current MinHeap size [%s] must be greater than [%s]", minHeapSizeGb, MIN_HEAP_SIZE_GB);
      checkArgument(maxHeapSizeGb >= MIN_HEAP_SIZE_GB, "The current MaxHeap size [%s] must be greater than [%s]", maxHeapSizeGb, MIN_HEAP_SIZE_GB);
    }

    private void checkDirectMemoryMinReq(long numCalls){
      val estimate = BYTES_PER_CALL*numCalls;
      val actual = directMemorySize * (1 << 30);
      checkArgument(actual >= estimate,
          "For the numCalls [%s], the estimated direct memory size is [%s] bytes, but the actual configuration was less ([%s] bytes). Inorder to run this, you need to increase that direct memory capacity so that the direct memory is >= BytesPerCall(%s) * NumCalls(%s)",
          numCalls, estimate, actual, BYTES_PER_CALL, numCalls);
    }

  }

}
