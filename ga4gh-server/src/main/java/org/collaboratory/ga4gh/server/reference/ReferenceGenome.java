/*
 * Copyright (c) 2016 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.collaboratory.ga4gh.server.reference;

import static com.google.common.io.Files.getNameWithoutExtension;

import java.io.File;

import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.SAMSequenceRecord;
import htsjdk.samtools.reference.IndexedFastaSequenceFile;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ReferenceGenome {

  /**
   * The reference assembly version that corresponds to the configured {@link #sequenceFile}.
   */
  @NonNull
  private final String assemblyVersion;

  /**
   * The FASTA file used for validation.
   */
  @NonNull
  private final IndexedFastaSequenceFile sequenceFile;

  /**
   * Creates a {@code PicardReferenceGenome} configured with the supplied {@code fastaFilePath}.
   * 
   * @param fastaFilePath the fully qualified path to the the {@code .fasta} file. Expected to be placed next to
   * {@code .fai} file with the same prefix.
   */
  @SneakyThrows
  public ReferenceGenome(@NonNull String fastaFilePath) {
    val fastaFile = new File(fastaFilePath).getAbsoluteFile();
    this.assemblyVersion = getAssemblyVersion(fastaFile);
    this.sequenceFile = new IndexedFastaSequenceFile(fastaFile);

    log.info("Using '{}' assembly versioned FASTA file: '{}'", assemblyVersion, fastaFile);
  }

  public String getVersion() {
    return assemblyVersion;
  }

  public SAMSequenceDictionary getDictionary() {
    return sequenceFile.getSequenceDictionary();
  }

  public SAMSequenceRecord getReference(String referenceId) {
    return sequenceFile.getSequenceDictionary().getSequence(referenceId);
  }

  public String getSequence(String referenceId, long startPosition, long endPosition) {
    return get(referenceId, startPosition, endPosition);
  }

  private String get(String referenceId, long start, long end) {
    try {
      val sequence = sequenceFile.getSubsequenceAt(referenceId, start, end);
      val text = new String(sequence.getBases());
      return text;
    } catch (Exception e) {
      throw new RuntimeException(
          "Error trying to get subsequence at: chr=" + referenceId + " start=" + start + ", end=" + end, e);
    }
  }

  /**
   * Extracts the assembly version from the file name.
   * 
   * @param fastaFile the FASTA file
   * @return the assembly version
   */
  private static String getAssemblyVersion(File fastaFile) {
    return getNameWithoutExtension(fastaFile.getName());
  }

}
