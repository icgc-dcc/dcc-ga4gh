package org.collaboratory.ga4gh.loader.factory;

import lombok.NonNull;
import org.collaboratory.ga4gh.loader.model.metadata.fetcher.Fetcher;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static org.collaboratory.ga4gh.loader.model.metadata.fetcher.decorators.LimitFetcherDecorator.newLimitFetcherDecorator;
import static org.collaboratory.ga4gh.loader.model.metadata.fetcher.decorators.MaxFileSizeFetcherDecorator.newMaxFileSizeFetcherDecorator;
import static org.collaboratory.ga4gh.loader.model.metadata.fetcher.decorators.OrderFetcherDecorator.newShuffleFetcherDecoratorWithSeed;
import static org.collaboratory.ga4gh.loader.model.metadata.fetcher.decorators.OrderFetcherDecorator.newSizeSortingFetcherDecorator;
import static org.collaboratory.ga4gh.loader.model.metadata.fetcher.decorators.SSMFetcherDecorator.newSSMFetcherDecorator;
import static org.collaboratory.ga4gh.loader.model.metadata.fetcher.impl.AllFetcher.newAllFetcher;
import static org.collaboratory.ga4gh.loader.model.metadata.fetcher.impl.AllFetcher.newAllFetcherDefaultStorageFilename;
import static org.collaboratory.ga4gh.loader.model.metadata.fetcher.impl.NumDonorsFetcher.newNumDonorsFetcher;

public class FetcherFactory {
  private boolean enableSorting = false;
  private boolean sortAscending = false;
  private long shuffleSeed = -1;
  private int limit = -1;
  private long maxFileSizeBytes = -1;

  private boolean enableLimiting = false;
  private boolean enableMaxFileSizeBytes = false;

  private boolean enableNumDonors = false;
  private int numDonors = -1;

  private boolean enableAllFiles = false;
  private String storageFilename = "target/allData.dat";
  private boolean forceNewFile = false;
  private boolean enableDefaultPersistance = false;


  private boolean enableSSMFiltering = false;

  public static FetcherFactory builder(){
    return new FetcherFactory();
  }

  public FetcherFactory setSort(final boolean sortAscending){
    return setSort(true, sortAscending);
  }

  public FetcherFactory setSort(final boolean enableSorting, final boolean sortAscending){
    this.enableSorting = enableSorting;
    this.sortAscending = sortAscending;
    return this;
  }

  public FetcherFactory setShuffle(final long shuffleSeed){
    return setShuffle(true, shuffleSeed);
  }

  public FetcherFactory setShuffle(final boolean enableShuffling, final long shuffleSeed){
    this.enableSorting = ! enableShuffling;
    this.shuffleSeed = shuffleSeed;
    return this;
  }

  public FetcherFactory setLimit(final int limit){
    return setLimit(true, limit);
  }

  public FetcherFactory setLimit(final boolean enableLimiting, final int limit){
    this.enableLimiting = enableLimiting;
    this.limit = limit;
    checkArgument(limit > 0);
    return this;
  }

  public FetcherFactory setMaxFileSizeBytes(final long maxFileSizeBytes){
    return setMaxFileSizeBytes(true, maxFileSizeBytes);
  }

  public FetcherFactory setMaxFileSizeBytes(final boolean enableMaxFileSizeBytes , final long maxFileSizeBytes){
    this.enableMaxFileSizeBytes = enableMaxFileSizeBytes;
    this.maxFileSizeBytes = maxFileSizeBytes;
    checkArgument(maxFileSizeBytes > 0);
    return this;
  }

  public FetcherFactory setNumDonors(final boolean enableNumDonors, final int numDonors){
    this.enableNumDonors = enableNumDonors;
    this.numDonors = numDonors;
    checkArgument(numDonors > 0);
    return this;
  }

  public FetcherFactory setSSMFiltering(final boolean enableSSMFiltering){
    this.enableSSMFiltering = enableSSMFiltering;
    return this;
  }

  public FetcherFactory setNumDonors(final int numDonors){
    return setNumDonors(true, numDonors);
  }

  public FetcherFactory setAllFiles(final String storageFilename, final boolean forceNewFile){
    return setAllFiles(true, storageFilename, forceNewFile);
  }

  public FetcherFactory setAllFiles(final boolean enableAllFiles, @NonNull final String storageFilename, final boolean forceNewFile){
    this.enableAllFiles = enableAllFiles;
    this.enableDefaultPersistance = false;
    this.storageFilename = storageFilename;
    this.forceNewFile = forceNewFile;
    return this;
  }

  public FetcherFactory setAllFiles(final boolean forceNewFile){
    return setAllFiles(true, forceNewFile);
  }

  public FetcherFactory setAllFiles(final boolean enableAllFiles, final boolean forceNewFile){
    this.enableAllFiles = enableAllFiles;
    this.enableDefaultPersistance = true;
    this.forceNewFile = forceNewFile;
    return this;
  }


  public Fetcher build(){
    Fetcher fetcher;
    checkState(enableAllFiles != enableNumDonors, "enableAllFiles [{}] cannot match the value of enableNumDonors [{}]",
        enableAllFiles,
        enableNumDonors);
    if (enableAllFiles){
      if (enableDefaultPersistance){
        fetcher = newAllFetcherDefaultStorageFilename(forceNewFile);
      } else {
        fetcher = newAllFetcher(storageFilename, forceNewFile);
      }
    } else { //enableNumDonors
      fetcher = newNumDonorsFetcher(numDonors);
    }


    if (enableSorting){
      fetcher  = newSizeSortingFetcherDecorator(fetcher, sortAscending);
    }else{
      fetcher = newShuffleFetcherDecoratorWithSeed(fetcher, shuffleSeed);
    }

    if (enableMaxFileSizeBytes){
      fetcher = newMaxFileSizeFetcherDecorator(fetcher, maxFileSizeBytes);
    }

    if (enableLimiting){
      fetcher = newLimitFetcherDecorator(fetcher, limit);
    }

    if (enableSSMFiltering){
      fetcher = newSSMFetcherDecorator(fetcher);

    }
    return fetcher;
  }

}
