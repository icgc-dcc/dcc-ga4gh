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
package org.collaboratory.ga4gh.loader;

import static lombok.AccessLevel.PRIVATE;
import static org.collaboratory.ga4gh.loader.FileMetaData.filter;

import java.util.List;

import org.collaboratory.ga4gh.loader.enums.MutationTypes;
import org.collaboratory.ga4gh.loader.enums.SubMutationTypes;

import lombok.NoArgsConstructor;
import lombok.NonNull;

/*
 * Functions for filtering lists of FileMetaDatas
 */
@NoArgsConstructor(access = PRIVATE)
public class FileMetaDataFilters {

  /*
   * Filters input list of FileMetaDatas to be less than specified size
   */
  public static List<FileMetaData> filterBySize(@NonNull final Iterable<FileMetaData> fileMetaDatas,
      final long maxSizeBytes) {
    return filter(fileMetaDatas, f -> f.getFileSize() < maxSizeBytes);
  }

  /*
   * Filters input list of FileMetaDatas by MutationType==somatic, SubMutationType==(snv_mnv or indel)
   */
  public static List<FileMetaData> filterSomaticSSMs(@NonNull final Iterable<FileMetaData> fileMetaDatas) {
    return filter(fileMetaDatas, f -> isSomaticSSM(f));
  }

  /*
   * Checks if FileMetaData is classified as somatic and indel or snv_mnv
   */
  public static boolean isSomaticSSM(final FileMetaData f) {
    return f.compare(MutationTypes.somatic)
        && (f.compare(SubMutationTypes.indel)
            || f.compare(SubMutationTypes.snv_mnv));
  }

}
