package org.icgc.dcc.ga4gh.loader.model.metadata.fetcher.decorators;

import com.google.common.collect.Maps;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import org.icgc.dcc.ga4gh.loader.model.metadata.FileMetaData;
import org.icgc.dcc.ga4gh.loader.model.metadata.FileMetaDataContext;
import org.icgc.dcc.ga4gh.loader.model.metadata.fetcher.Fetcher;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;

/**
 * TODO: ditch Fetcher dependancy, and directly depend on FileMetaDataContext, and just make this decorator a decorator for FileMetaDataContext
 */
@RequiredArgsConstructor
public class SelectedFilesFetcherDecorator implements Fetcher {

  public static SelectedFilesFetcherDecorator newSelectedFilesFetcherDecorator(final Fetcher fetcher, final List<String> orderedFilenames){
    return new SelectedFilesFetcherDecorator(fetcher, orderedFilenames);
  }

  @NonNull
  private final Fetcher fetcher;

  @NonNull
  private final List<String> orderedFilenames;

  @Override
  @SneakyThrows
  public FileMetaDataContext fetch() {
    val fileMetaDataContext = fetcher.fetch();
    val map = Maps.<String, FileMetaData>newHashMap();
    for(val f : fileMetaDataContext){
      val fn = f.getVcfFilenameParser().getFilename();
      checkState(! map.containsKey(fn), "The map already containsObject the key {}. These means there are duplicate files names, when the assumption is that they are unique", fn);
      map.put(fn, f);
    }

    val builder = FileMetaDataContext.builder();
    for (val filename : orderedFilenames){
      if (map.containsKey(filename)){
        val fileMetaData = map.get(filename);
        builder.fileMetaData(fileMetaData);
      }
    }
    return builder.build();
  }
}
