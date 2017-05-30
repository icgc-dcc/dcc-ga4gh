package org.icgc.dcc.ga4gh.loader.portal;

import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.icgc.dcc.ga4gh.common.ObjectNodeConverter;

import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.common.core.json.JsonNodeBuilders.object;

@RequiredArgsConstructor(access = PRIVATE)
@Value
@Slf4j
public class PortalAllCollabVcfFileQueryCreator implements ObjectNodeConverter {

  @Override
  public ObjectNode toObjectNode(){
    return object()
        .with("file",
            object()
                .with("repoName", createIs("Collaboratory - Toronto"))
                .with("dataType", createIs("SSM"))
                .with("study", createIs("PCAWG"))
                .with("experimentalStrategy", createIs("WGS"))
                .with("fileFormat", createIs("VCF"))
        )
        .end();
  }

  public static PortalAllCollabVcfFileQueryCreator createPortalAllCollabVcfFileQueryCreator() {
    log.info("Creating PortalAllCollabVcfFileQueryCreator instance");
    return new PortalAllCollabVcfFileQueryCreator();
  }

}
