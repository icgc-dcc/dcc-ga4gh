package org.icgc.dcc.ga4gh.loader2.dao.portal;

import com.google.common.collect.ImmutableList;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.icgc.dcc.ga4gh.common.model.portal.PortalMetadata;
import org.icgc.dcc.ga4gh.loader2.dao.BasicDao;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.groupingBy;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;

@RequiredArgsConstructor
public class PortalMetadataDao implements BasicDao<PortalMetadata, PortalMetadataRequest>, Serializable {

  public static final long serialVersionUID = 1492088726L;

  public static final PortalMetadataDao newPortalMetadataDao(List<PortalMetadata> data){
    return new PortalMetadataDao(data);
  }

  @NonNull private final List<PortalMetadata> data;

  private Stream<PortalMetadata> getStream(PortalMetadataRequest request){
    return data.stream()
        .filter(x -> x.getPortalFilename().equals(request.getPortalFilename()));
  }

  @Override public List<PortalMetadata> find(PortalMetadataRequest request) {
    return getStream(request).collect(toImmutableList());
  }

  @Override public List<PortalMetadata> findAll() {
    return ImmutableList.copyOf(data);
  }

  public Optional<PortalMetadata> findFirst(PortalMetadataRequest request){
    return getStream(request).findFirst();
  }

  public Map<String, List<PortalMetadata>> groupBySampleId(){
    return groupBy(PortalMetadata::getSampleId);
  }

  public <T> Map<T, List<PortalMetadata>> groupBy(Function<PortalMetadata, T> functor){
    return findAll().stream()
        .collect(groupingBy(functor));
  }


}
