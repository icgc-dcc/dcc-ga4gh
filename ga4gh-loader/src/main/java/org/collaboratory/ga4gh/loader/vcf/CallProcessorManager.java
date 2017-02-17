package org.collaboratory.ga4gh.loader.vcf;

import lombok.NonNull;
import lombok.val;
import org.collaboratory.ga4gh.loader.vcf.callprocessors.CallProcessor;
import org.collaboratory.ga4gh.loader.vcf.enums.CallerTypes;

import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.newHashMap;
import static org.icgc.dcc.common.core.util.stream.Streams.stream;

public class CallProcessorManager {

  private final Map<CallerTypes, CallProcessor> map = newHashMap();

  public static CallProcessorManager newCallProcessorManager() {
    return new CallProcessorManager();
  }

  public CallProcessorManager addCallProcessor(CallProcessor processor,
      @NonNull CallerTypes... callerTypes) {
    stream(callerTypes).forEach(c -> addCallProcessor(processor, c));
    return this;
  }

  public CallProcessorManager addCallProcessor(CallProcessor processor,
      @NonNull Iterable<CallerTypes> callerTypes) {
    stream(callerTypes).forEach(c -> addCallProcessor(processor, c));
    return this;
  }

  public CallProcessorManager addCallProcessor(@NonNull CallProcessor processor, @NonNull CallerTypes callerType) {
    checkState(!map.containsKey(callerType),
        "The mapping of any callerType to CallProcessor is a many to one relationship. The callerType [%s] cannot be mapped to the instance of class [%s]",
        callerType, processor.getClass().getName());
    map.put(callerType, processor);
    return this;
  }

  public CallProcessor getCallProcessor(@NonNull CallerTypes callerType) {
    val callerTypeHasCallProcess = map.containsKey(callerType);
    checkState(callerTypeHasCallProcess, "The callerType [%s] does not have a callProcessor mapped to it", callerType);
    return map.get(callerType);
  }

}
