package org.icgc.dcc.ga4gh.loader2;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.ga4gh.loader2.persistance.LocalFileRestorerFactory;
import org.icgc.dcc.ga4gh.loader2.storage.StorageFactory;
import org.junit.Test;

import java.nio.file.Paths;

import static org.icgc.dcc.ga4gh.loader.Config.STORAGE_OUTPUT_VCF_STORAGE_DIR;
import static org.icgc.dcc.ga4gh.loader.Config.TOKEN;
import static org.icgc.dcc.ga4gh.loader2.dao.portal.PortalMetadataDaoFactory.newDefaultPortalMetadataDaoFactory;
import static org.icgc.dcc.ga4gh.loader2.portal.PortalCollabVcfFileQueryCreator.newPortalCollabVcfFileQueryCreator;

@Slf4j
public class DaoTest {

  @Test
  public void testDao(){
    val storage = StorageFactory.builder()
        .bypassMD5Check(false)
        .outputVcfDir(Paths.get(STORAGE_OUTPUT_VCF_STORAGE_DIR))
        .persistVcfDownloads(true)
        .token(TOKEN)
        .build()
        .getStorage();

    val localFileRestorerFactory = LocalFileRestorerFactory.newFileRestorerFactory("test.persisted");
    val query = newPortalCollabVcfFileQueryCreator();
    val portalMetadataDaoFactory = newDefaultPortalMetadataDaoFactory(localFileRestorerFactory, query);
    val portalMetadataDao = portalMetadataDaoFactory.createPortalMetadataDao();
    for (val portalMetadata : portalMetadataDao.findAll()){
      val file = storage.getFile(portalMetadata);
      log.info("Downloaded: {}", portalMetadata.getPortalFilename().getFilename());



    }

  }

}
