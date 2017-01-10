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
package org.collaboratory.ga4gh.loader.metadata;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import lombok.val;

public class FileMetaDataFetcherTest {

  @Test
  public void testShuffler() {
    int numDonors = 30;
    val dataFetcherShuffle1 = FileMetaDataFetcher.builder()
        .numDonors(numDonors)
        // .maxFileSizeBytes(DEBUG_FILEMETADATA_MAX_SIZE)
        .somaticSSMsOnly(true)
        .shuffle(true)
        .build();
    val dataFetcherShuffle2 = FileMetaDataFetcher.builder()
        .numDonors(numDonors)
        // .maxFileSizeBytes(DEBUG_FILEMETADATA_MAX_SIZE)
        .somaticSSMsOnly(true)
        .shuffle(true)
        .build();
    val dataFetcherShuffle2_withSeed2 = FileMetaDataFetcher.builder()
        .numDonors(numDonors)
        // .maxFileSizeBytes(DEBUG_FILEMETADATA_MAX_SIZE)
        .somaticSSMsOnly(true)
        .seed(dataFetcherShuffle2.getSeed())
        .shuffle(true)
        .build();

    val dataFetcherNonShuffle1 = FileMetaDataFetcher.builder()
        .numDonors(numDonors)
        // .maxFileSizeBytes(DEBUG_FILEMETADATA_MAX_SIZE)
        .somaticSSMsOnly(true)
        .shuffle(false)
        .build();
    val dataFetcherNonShuffle2 = FileMetaDataFetcher.builder()
        .numDonors(numDonors)
        // .maxFileSizeBytes(DEBUG_FILEMETADATA_MAX_SIZE)
        .somaticSSMsOnly(true)
        .shuffle(false)
        .build();

    val fmdShuffle1 = dataFetcherShuffle1.fetch();
    val fmdShuffle2 = dataFetcherShuffle2.fetch();
    val fmdShuffle2_withSeed2 = dataFetcherShuffle2_withSeed2.fetch();
    val fmdNonShuffle1 = dataFetcherNonShuffle1.fetch();
    val fmdNonShuffle2 = dataFetcherNonShuffle2.fetch();

    // Assert that 2 dataFetchers with same parameters except for SEED values, and shuffle ON,
    // result in FileMetaData lists that are DIFFERENT, therefore NOT EQUAL
    assertThat(!fmdShuffle1.equals(fmdShuffle2));

    // Assert that 2 dataFetchers with same parameters, regardless of SEED values, and shuffle OFF,
    // result in FileMetaData lists that are the SAME, therefore EQUAL
    assertThat(fmdNonShuffle1.equals(fmdNonShuffle2));

    // Assert that 2 dataFetchers with same parameters, included same SEED values, and shuffle ON,
    // result in FileMetaData lists that are the SAME, therefore EQUAL
    assertThat(fmdShuffle2.equals(fmdShuffle2_withSeed2));
  }
}
