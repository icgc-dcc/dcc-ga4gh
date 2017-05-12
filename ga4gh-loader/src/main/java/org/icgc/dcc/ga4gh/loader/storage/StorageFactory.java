package org.icgc.dcc.ga4gh.loader.storage;

import lombok.Builder;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.nio.file.Path;

import static org.icgc.dcc.ga4gh.loader.storage.impl.PortalStorage.newPortalStorage;

@RequiredArgsConstructor
@Builder
public class StorageFactory {

  @NonNull  private final Path outputVcfDir;
  private final boolean bypassMD5Check;
  @NonNull private final String token;
  private final boolean persistVcfDownloads;

  public Storage getStorage(){
      return newPortalStorage(persistVcfDownloads, outputVcfDir, bypassMD5Check, token);
  }


}
