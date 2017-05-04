package org.icgc.dcc.ga4gh.common.model.es;

import com.google.common.collect.ImmutableList;

import java.util.List;

public interface EsVariant2 {

  List<EsCall> getCalls();

  int getStart();

  int getEnd();

  String getReferenceName();

  String getReferenceBasesAsString();

  ImmutableList<String> getAlternativeBasesAsStrings();

  int numCalls();

  EsVariant2 setStart(int start);
  EsVariant2 setEnd(int end);
  EsVariant2 setReferenceName(String referenceName);
  EsVariant2 setReferenceBases(String referenceBases);
  EsVariant2 setReferenceBases(byte[] referenceBases);
  EsVariant2 setAlternativeBases(Iterable<? extends String> inputAlternativeBases);
  EsVariant2 setAlternativeBases(byte[][] inputAlternativeBases);
}
