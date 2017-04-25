package org.icgc.dcc.ga4gh.loader2.dao.portal;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import org.icgc.dcc.ga4gh.common.ObjectNodeConverter;
import org.icgc.dcc.ga4gh.common.model.portal.PortalMetadata;
import org.icgc.dcc.ga4gh.loader2.persistance.LocalFileRestorerFactory;
import org.icgc.dcc.ga4gh.loader2.portal.Portal;
import org.icgc.dcc.ga4gh.loader2.portal.PortalFiles;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;
import static org.icgc.dcc.ga4gh.loader2.dao.portal.PortalMetadataDao.newPortalMetadataDao;

@RequiredArgsConstructor
public class PortalMetadataDaoFactory {

  public static PortalMetadataDaoFactory newPortalMetadataDaoFactory(
      LocalFileRestorerFactory localFileRestorerFactory, Portal portal, String persistanceName) {
    return new PortalMetadataDaoFactory(localFileRestorerFactory, portal, persistanceName);
  }

  public static PortalMetadataDaoFactory newDefaultPortalMetadataDaoFactory(
      LocalFileRestorerFactory localFileRestorerFactory, ObjectNodeConverter query) {
    val portal = Portal.builder().jsonQueryGenerator(query).build();
    val persistanceName = PortalMetadataDao.class.getSimpleName();
    return newPortalMetadataDaoFactory(localFileRestorerFactory, portal, persistanceName);
  }

  @NonNull private final LocalFileRestorerFactory localFileRestorerFactory;
  @NonNull private final Portal portal;
  @NonNull private final String persistanceName;


  @SneakyThrows
  public PortalMetadataDao createPortalMetadataDao(){
    val restorer = localFileRestorerFactory.<ArrayList<PortalMetadata>>createFileRestorerByName(persistanceName);
    ArrayList<PortalMetadata> inputList = null;
    if (restorer.isPersisted()){
      inputList = restorer.restore();
    } else {
      inputList = newArrayList(fetchData());
      restorer.store(inputList);
    }
    return newPortalMetadataDao(inputList);
  }

  private List<PortalMetadata> fetchData(){
    return portal.getFileMetas()
        .stream()
        .map(PortalFiles::convertToPortalMetadata)
        .collect(toImmutableList());
  }

}
