package org.icgc.dcc.ga4gh.common.model.es;

import com.google.common.collect.ImmutableList;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.experimental.NonFinal;

import java.util.List;

import static com.google.common.collect.Lists.newArrayList;

@RequiredArgsConstructor
@Value
public class EsWithCallsVariant2 implements EsVariant2 {

  @NonNull private final EsNoCallsVariant2 esNoCallsVariant2;

  @NonFinal
  private List<EsCall> calls = newArrayList();

  public EsWithCallsVariant2 setCalls(List<EsCall> calls){
    this.calls = calls;
    return this;
  }

  public EsWithCallsVariant2 addCall(EsCall call){
    calls.add(call);
    return this;
  }

  @Override public List<EsCall> getCalls(){
    return this.calls;
  }

  @Override public int getStart() {
    return esNoCallsVariant2.getStart();
  }

  @Override public int getEnd() {
    return esNoCallsVariant2.getEnd();
  }

  @Override public String getReferenceName() {
    return esNoCallsVariant2.getReferenceName();
  }

  @Override public String getReferenceBasesAsString() {
    return esNoCallsVariant2.getReferenceBasesAsString();
  }

  @Override public ImmutableList<String> getAlternativeBasesAsStrings() {
    return esNoCallsVariant2.getAlternativeBasesAsStrings();
  }

  @Override public EsVariant2 setStart(int start) {
    return esNoCallsVariant2.setStart(start);
  }

  @Override public EsVariant2 setEnd(int end) {
    return esNoCallsVariant2.setEnd(end);
  }

  @Override public int numCalls() {
    return esNoCallsVariant2.numCalls();
  }

  @Override public EsVariant2 setReferenceName(String referenceName) {
    return esNoCallsVariant2.setReferenceName(referenceName);
  }

  @Override public EsVariant2 setReferenceBases(String referenceBases) {
    return esNoCallsVariant2.setReferenceBases(referenceBases);
  }

  @Override public EsVariant2 setReferenceBases(byte[] referenceBases) {
    return esNoCallsVariant2.setReferenceBases(referenceBases);
  }

  @Override public EsVariant2 setAlternativeBases(Iterable<? extends String> inputAlternativeBases) {
    return esNoCallsVariant2.setAlternativeBases(inputAlternativeBases);
  }

  @Override public EsVariant2 setAlternativeBases(byte[][] inputAlternativeBases) {
    return esNoCallsVariant2.setAlternativeBases(inputAlternativeBases);
  }

}
