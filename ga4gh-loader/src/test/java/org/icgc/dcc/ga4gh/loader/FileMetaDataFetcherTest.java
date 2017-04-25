/*
 * Copyright (c) 2017 The Ontario Institute for Cancer Research. All rights reserved.                             
 *                                                                                                               
 * This program and the accompanying materials are made available under the terms of the GNU Public License v3.0.
 * You should have received a copy of the GNU General Public License along with                                  
 * this program. If not, see <http://www.gnu.org/licenses/>.                                                     
 *                                                                                                               
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY                           
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES                          
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT                           
 * SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,                                
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED                          
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;                               
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER                              
 * IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN                         
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.icgc.dcc.ga4gh.loader;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Sets;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.ga4gh.loader.model.metadata.FileMetaDataContext;
import org.icgc.dcc.ga4gh.loader.model.metadata.fetcher.decorators.OrderFetcherDecorator;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.ga4gh.loader.model.metadata.fetcher.decorators.LimitFetcherDecorator.newLimitFetcherDecorator;
import static org.icgc.dcc.ga4gh.loader.model.metadata.fetcher.decorators.MaxFileSizeFetcherDecorator.newMaxFileSizeFetcherDecorator;
import static org.icgc.dcc.ga4gh.loader.model.metadata.fetcher.impl.AllFetcher.newAllFetcherDefaultStorageFilename;
import static org.icgc.dcc.ga4gh.loader.model.metadata.fetcher.impl.NumDonorsFetcher.newNumDonorsFetcher;
import static org.icgc.dcc.common.core.util.stream.Streams.stream;

@Slf4j
public class FileMetaDataFetcherTest {

  @Test
  @SneakyThrows
  public void testPersistance() {
    final int numDonors = 8;
    val filename = "target/fileMetaDatas.testPersistance.bin";
    val fetcher = newNumDonorsFetcher(numDonors);
    val ctxOriginal = fetcher.fetch();
    ctxOriginal.store(filename);

    val ctxRestored = FileMetaDataContext.restore(filename);
    verifyFileMetaDataContextsContents(ctxOriginal, ctxRestored);
  }

  /**
   * This test takes several minutes, so ignoring since will make tests really really long
   */
  @Test
  @SneakyThrows
  @Ignore
  public void testAllFetcher() {
    boolean FORCE_NEW_STORAGE_FILE = true;
    val fetcher1 = newAllFetcherDefaultStorageFilename(FORCE_NEW_STORAGE_FILE);
    val watch1 = Stopwatch.createStarted();
    val ctx1 = fetcher1.fetch();
    watch1.stop();

    val fetcher2 = newAllFetcherDefaultStorageFilename(! FORCE_NEW_STORAGE_FILE);
    val watch2 = Stopwatch.createStarted();
    val ctx2 = fetcher2.fetch();
    watch2.stop();

    verifyFileMetaDataContextsContents(ctx1, ctx2);
    assertThat(watch1.elapsed(TimeUnit.SECONDS)).isGreaterThan(watch2.elapsed(TimeUnit.SECONDS)); //Not the best test, but should be considerable speed increase when reading from file.
  }

  @Test
  @SneakyThrows
  public void testNumDonorsFetcher() {
    val numDonors = 8;
    val fetcher1 = newNumDonorsFetcher(numDonors);
    val ctx1 = fetcher1.fetch();
    val fetcher2 = newNumDonorsFetcher(numDonors);
    val ctx2 = fetcher2.fetch();
    verifyFileMetaDataContextsContents(ctx1, ctx2);
  }

  private void verifyFileMetaDataContextsContents(final FileMetaDataContext ctx1, final FileMetaDataContext ctx2){
    assertThat(ctx1.size() > 0);
    assertThat(ctx2.size() > 0);
    val set1 = Sets.newHashSet(ctx1);
    val set2 = Sets.newHashSet(ctx2);
    assertThat(set1.size()).isEqualTo(ctx1.size());
    assertThat(set2.size()).isEqualTo(ctx2.size());
    assertThat(set1.containsAll(set2));
    assertThat(set2.containsAll(set1));
  }

  @Test
  @SneakyThrows
  public void testOrderingFileSizeAscending()  {
    orderFileSizeTest(8, true);
  }

  @Test
  @SneakyThrows
  public void testOrderingFileSizeDescending()  {
    orderFileSizeTest(8, false);
  }


  @SneakyThrows
  private void orderFileSizeTest(final int numDonors, final boolean sortAscending ) {
    val fetcher = newNumDonorsFetcher(numDonors);
    val orderDecorator = OrderFetcherDecorator.newSizeSortingFetcherDecorator(fetcher, sortAscending);
    boolean first = true;
    long fileSize = -1;
    long prevFileSize = -1;
    val ctx  = orderDecorator.fetch();
    for (val f : ctx){
      prevFileSize = fileSize;
      fileSize = f.getFileSize();
      if (first){
        first = false;
      } else if (! sortAscending){
        assertThat(fileSize).isLessThanOrEqualTo(prevFileSize);
      } else if (sortAscending){
        assertThat(fileSize).isGreaterThan(prevFileSize);
      } else {
        throw new IllegalStateException("There should be no other option");
      }
    }
  }

  @Test
  @SneakyThrows
  public void testShufflingSameSeed()  {
    val numDonors = 8;
    val fetcher = newNumDonorsFetcher(numDonors);
    val seed = OrderFetcherDecorator.generateSeed();
    val orderDecorator1 = OrderFetcherDecorator.newShuffleFetcherDecoratorWithSeed(fetcher, seed );
    val orderDecorator2 = OrderFetcherDecorator.newShuffleFetcherDecoratorWithSeed(fetcher, seed );
    val ctx1 = orderDecorator1.fetch();
    val ctx2 = orderDecorator2.fetch();
    verifyFileMetaDataContextsContents(ctx1, ctx2);
    val size = ctx1.size();

    for (int i=0; i< size; i++){
     val e1 =  ctx1.getFileMetaDatas().get(i);
     val e2 =  ctx2.getFileMetaDatas().get(i);
     assertThat(e1.equals(e2));
    }
  }

  @Test
  @SneakyThrows
  public void testShufflingDifferentSeed()  {
    val numDonors = 8;
    val fetcher = newNumDonorsFetcher(numDonors);
    val seed1 = OrderFetcherDecorator.generateSeed();
    val seed2 = seed1 - 31984;
    val orderDecorator1 = OrderFetcherDecorator.newShuffleFetcherDecoratorWithSeed(fetcher, seed1 );
    val orderDecorator2 = OrderFetcherDecorator.newShuffleFetcherDecoratorWithSeed(fetcher, seed2 );
    val ctx1 = orderDecorator1.fetch();
    val ctx2 = orderDecorator2.fetch();
    verifyFileMetaDataContextsContents(ctx1, ctx2);
    val size = ctx1.size();

    int numMismatches = 0;
    for (int i=0; i< size; i++){
      val e1 =  ctx1.getFileMetaDatas().get(i);
      val e2 =  ctx2.getFileMetaDatas().get(i);
      if (!e1.equals(e2)){
        numMismatches++;
      }
    }
    assertThat(numMismatches).isGreaterThan(2);
  }

  @Test
  @SneakyThrows
  public void testOrderFilename(){
    val numDonors = 8;
    val fetcher = newNumDonorsFetcher(numDonors);
    val sortAscending = true;

    // Shuffle one fetcher, then sort
    val shuffleDecorator1 = OrderFetcherDecorator.newShuffleFetcherDecorator(fetcher);
    val orderDecorator1 = OrderFetcherDecorator.newFilenameSortingFetcherDecorator(shuffleDecorator1, sortAscending);

    // Shuffle second fetcher, then sort
    val shuffleDecorator2 = OrderFetcherDecorator.newShuffleFetcherDecorator(fetcher);
    val orderDecorator2 = OrderFetcherDecorator.newFilenameSortingFetcherDecorator(shuffleDecorator2, sortAscending);

    // Then compare them, and order should be preserved
    val ctx1 = orderDecorator1.fetch();
    val ctx2 = orderDecorator2.fetch();
    verifyFileMetaDataContextsContents(ctx1, ctx2);

    val size = ctx1.size();
    for (int i=0; i< size; i++){
      val e1 =  ctx1.getFileMetaDatas().get(i);
      val e2 =  ctx2.getFileMetaDatas().get(i);
      assertThat(e1.equals(e2));
    }
  }

  @Test
  @SneakyThrows
  public void testMaxFileSizeFetcher(){
    val numDonors = 8;
    val fetcher = newNumDonorsFetcher(numDonors);
    val maxFileSizeBytes = 10*1024*1024L; //10 MB
    val maxFilesizeDecorator = newMaxFileSizeFetcherDecorator(fetcher,  maxFileSizeBytes);
    val ctx = maxFilesizeDecorator.fetch();
    val numFilesGreaterThanLimit = stream(ctx).filter(x -> x.getFileSize() >= maxFileSizeBytes).count();
    assertThat(numFilesGreaterThanLimit == 0).describedAs("The number of files greater than {} should be 0", maxFileSizeBytes);
  }

  @Test
  @SneakyThrows
  public void testLimitFetcher(){
    val numDonors = 8;
    val limit = 10;
    val fetcher = newNumDonorsFetcher(numDonors);
    val ctxOrig = fetcher.fetch();

    assertThat(ctxOrig.size()).isGreaterThan(limit);

    val limitFetcher = newLimitFetcherDecorator(fetcher,  limit);
    val ctxLimit = limitFetcher.fetch();
    assertThat(ctxLimit.size()).isEqualTo(limit);

    val tooBigLimitFetcher = newLimitFetcherDecorator(limitFetcher, limit+10);
    val ctxExtra = tooBigLimitFetcher.fetch();
    assertThat(ctxExtra.size()).isEqualTo(limit);
  }
}
