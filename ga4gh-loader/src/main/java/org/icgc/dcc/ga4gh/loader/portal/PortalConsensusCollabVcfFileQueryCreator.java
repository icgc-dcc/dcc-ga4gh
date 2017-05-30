package org.icgc.dcc.ga4gh.loader.portal;

import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.icgc.dcc.ga4gh.common.ObjectNodeConverter;

import java.util.Set;

import static com.google.common.collect.Sets.newHashSet;
import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.common.core.json.JsonNodeBuilders.object;
import static org.icgc.dcc.ga4gh.loader.utils.Strings.toStringArray;

@RequiredArgsConstructor(access = PRIVATE)
@Value
@Slf4j
public class PortalConsensusCollabVcfFileQueryCreator implements ObjectNodeConverter {

  private static final Set<String> CONSENSUS_SOFTWARE_NAMES = newHashSet( "PCAWG SNV-MNV callers", "PCAWG InDel callers");

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
                .with("software", createIs(toStringArray(CONSENSUS_SOFTWARE_NAMES)))
        )
        .end();
  }

  public static PortalConsensusCollabVcfFileQueryCreator createPortalConsensusCollabVcfFileQueryCreator() {
    log.info("Creating PortalAllCollabVcfFileQueryCreator instance");
    return new PortalConsensusCollabVcfFileQueryCreator();
  }

}
