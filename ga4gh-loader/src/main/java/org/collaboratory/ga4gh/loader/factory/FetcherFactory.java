package org.collaboratory.ga4gh.loader.factory;

import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.collaboratory.ga4gh.loader.model.metadata.fetcher.Fetcher;
import org.collaboratory.ga4gh.loader.model.metadata.fetcher.impl.NumDonorsFetcher;

import static lombok.AccessLevel.PRIVATE;
import static org.collaboratory.ga4gh.loader.model.metadata.fetcher.decorators.OrderFetcherDecorator.newShuffleFetcherDecorator;
import static org.collaboratory.ga4gh.loader.model.metadata.fetcher.decorators.OrderFetcherDecorator.newSizeSortingFetcherDecorator;
import static org.collaboratory.ga4gh.loader.model.metadata.fetcher.impl.AllFetcher.newAllFetcher;

@Slf4j
@NoArgsConstructor(access = PRIVATE)
public class FetcherFactory {

  private static final String STORAGE_FILENAME = "target/allFileMetaDatas.bin";
  private static final boolean FORCE_NEW_FILE = false;


  public static Fetcher newSizeSortedNumDonorFetcher(final int numDonors, final boolean sortAscending){
    return newSizeSortingFetcherDecorator(
        new NumDonorsFetcher(numDonors)
        ,sortAscending);
  }

  public static Fetcher newSizeSortedAllFilesFetcher(final boolean sortAscending){
    return newSizeSortingFetcherDecorator(
        newAllFetcher(STORAGE_FILENAME,FORCE_NEW_FILE)
        ,sortAscending);
  }

  public static Fetcher newShuffledAllFilesFetcher(){
    return newShuffleFetcherDecorator(newAllFetcher(STORAGE_FILENAME, FORCE_NEW_FILE));
  }

  public static Fetcher newShuffledNumDonorsFetcher(final int numDonors){
    return newShuffleFetcherDecorator(new NumDonorsFetcher(numDonors));
  }
}
