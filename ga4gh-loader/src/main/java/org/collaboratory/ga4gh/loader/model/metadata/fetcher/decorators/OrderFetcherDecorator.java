package org.collaboratory.ga4gh.loader.model.metadata.fetcher.decorators;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.collaboratory.ga4gh.loader.model.metadata.FileMetaDataContext;
import org.collaboratory.ga4gh.loader.model.metadata.fetcher.Fetcher;

import java.io.IOException;

import static lombok.AccessLevel.PRIVATE;

enum OrderMode{
  SHUFFLE_MODE,
  SORT_FILENAME_MODE,
  SORT_FILE_SIZE_MODE;
}
@RequiredArgsConstructor(access = PRIVATE)
public class OrderFetcherDecorator implements Fetcher {

  public static OrderFetcherDecorator newSizeSortingFetcherDecorator(@NonNull final Fetcher fetcher, final boolean sortAscending){
    return new OrderFetcherDecorator(fetcher, OrderMode.SORT_FILE_SIZE_MODE, sortAscending, -1);
  }

  public static OrderFetcherDecorator newFilenameSortingFetcherDecorator(@NonNull final Fetcher fetcher, final boolean sortAscending){
    return new OrderFetcherDecorator(fetcher, OrderMode.SORT_FILENAME_MODE, sortAscending, -1);
  }

  public static OrderFetcherDecorator newShuffleFetcherDecoratorWithSeed(@NonNull final Fetcher fetcher, final long seed){
    return new OrderFetcherDecorator(fetcher, OrderMode.SHUFFLE_MODE, false, seed);
  }

  public static OrderFetcherDecorator newShuffleFetcherDecorator(@NonNull final Fetcher fetcher){
    return new OrderFetcherDecorator(fetcher, OrderMode.SHUFFLE_MODE, false, generateSeed());
  }
  public static final long generateSeed() {
    return System.currentTimeMillis();
  }

  @NonNull
  private final Fetcher fetcher;

  @NonNull
  private final OrderMode orderMode;

  private final boolean sortAscending;

  private final long shuffleSeed;

  //TODO: either shuffled, sorted. if was neither, then wouldnt use this class

  @Override
  public FileMetaDataContext fetch() throws IOException, ClassNotFoundException {
    val fileMetaDataContext = fetcher.fetch();

    if(orderMode == OrderMode.SHUFFLE_MODE){
      return fileMetaDataContext.shuffle(shuffleSeed);
    } else if(orderMode == OrderMode.SORT_FILE_SIZE_MODE){
      return fileMetaDataContext.sortByFileSize(sortAscending);
    } else if(orderMode == OrderMode.SORT_FILENAME_MODE){
      return fileMetaDataContext.sortByFilename(sortAscending);
    } else {
      throw new IllegalStateException(
          String.format("The mode [%s] is currently not apart of the mode state-machine", orderMode));
    }

  }

}
