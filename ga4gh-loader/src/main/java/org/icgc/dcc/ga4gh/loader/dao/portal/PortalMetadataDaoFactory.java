package org.icgc.dcc.ga4gh.loader.dao.portal;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import org.icgc.dcc.ga4gh.common.model.portal.PortalMetadata;
import org.icgc.dcc.ga4gh.loader.persistance.FileObjectRestorerFactory;
import org.icgc.dcc.ga4gh.loader.portal.Portal;
import org.icgc.dcc.ga4gh.loader.portal.PortalFiles;

import java.util.ArrayList;

import static com.google.common.collect.Lists.newArrayList;
import static java.util.stream.Collectors.toList;
import static org.icgc.dcc.ga4gh.loader.dao.portal.PortalMetadataDao.newPortalMetadataDao;

@RequiredArgsConstructor
public class PortalMetadataDaoFactory {

  public static PortalMetadataDaoFactory createPortalMetadataDaoFactory(String persistanceName, Portal portal,
      FileObjectRestorerFactory fileObjectRestorerFactory) {
    return new PortalMetadataDaoFactory(persistanceName, portal, fileObjectRestorerFactory);
  }

  @NonNull private final String persistanceName;
  @NonNull private final Portal portal;
  @NonNull private final FileObjectRestorerFactory fileObjectRestorerFactory;

  @SneakyThrows
  public PortalMetadataDao getPortalMetadataDao(){
    val list = fileObjectRestorerFactory.persistObject(persistanceName, this::createObject);
    return newPortalMetadataDao(list);
  }

  private ArrayList<PortalMetadata> createObject() {
    return newArrayList(portal.getFileMetas()
        .stream()
        .map(PortalFiles::convertToPortalMetadata)
        .collect(toList()));
  }

}
