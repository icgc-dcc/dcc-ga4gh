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
package org.collaboratory.ga4gh.loader.model.metadata;

import java.io.Serializable;
import java.util.Comparator;

import org.collaboratory.ga4gh.loader.PortalFiles;
import org.collaboratory.ga4gh.loader.PortalVCFFilenameParser;
import org.collaboratory.ga4gh.loader.enums.CallerTypes;
import org.collaboratory.ga4gh.loader.enums.MutationTypes;
import org.collaboratory.ga4gh.loader.enums.SubMutationTypes;

import com.fasterxml.jackson.databind.node.ObjectNode;

import lombok.Data;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

//TODO: [rtisma] -- consider storing the CallerTypes, MutationTypes and MutationSubTypes enum values instead of string representation. Or atleast keep strings, just create functions to compare the string against the enum
@Slf4j
@Data
public final class FileMetaData implements Serializable {

  private static final long serialVersionUID = 1484172786L;

  @NonNull
  private final String objectId;

  @NonNull
  private final String fileId;

  @NonNull
  private final String sampleId;

  @NonNull
  private final String donorId;

  @NonNull
  private final String dataType;

  @NonNull
  private final String referenceName;

  @NonNull
  private final String genomeBuild;

  private final long fileSize;

  @NonNull
  private final String fileMd5sum;

  @NonNull
  private final PortalVCFFilenameParser vcfFilenameParser;

  public static FileMetaData buildFileMetaData(@NonNull final ObjectNode objectNode) {
    val objectId = PortalFiles.getObjectId(objectNode);
    val fileId = PortalFiles.getFileId(objectNode);
    val sampleId = PortalFiles.getSampleId(objectNode);
    val donorId = PortalFiles.getDonorId(objectNode);
    val dataType = PortalFiles.getDataType(objectNode);
    val referenceName = PortalFiles.getReferenceName(objectNode);
    val genomeBuild = PortalFiles.getGenomeBuild(objectNode);
    val vcfFilenameParser = PortalFiles.getParser(objectNode);
    val fileSize = PortalFiles.getFileSize(objectNode);
    val fileMd5sum = PortalFiles.getFileMD5sum(objectNode);
    return new FileMetaData(objectId, fileId, sampleId, donorId, dataType, referenceName, genomeBuild, fileSize,
        fileMd5sum,
        vcfFilenameParser);
  }

  public boolean compare(final MutationTypes type) {
    return getVcfFilenameParser().getMutationType().equals(type.toString());
  }

  public boolean compare(final SubMutationTypes type) {
    return getVcfFilenameParser().getSubMutationType().equals(type.toString());
  }

  public boolean compare(final CallerTypes type) {
    return getVcfFilenameParser().getCallerId().equals(type.toString());
  }

  private static String getStartsWithRegex(final String keyword) {
    return "^" + keyword + ".*";
  }

  public boolean startsWith(final MutationTypes type) {
    return getVcfFilenameParser().getMutationType().matches(getStartsWithRegex(type.toString()));
  }

  public boolean startsWith(final SubMutationTypes type) {
    return getVcfFilenameParser().getSubMutationType().matches(getStartsWithRegex(type.toString()));
  }

  public boolean startsWith(final CallerTypes type) {
    return getVcfFilenameParser().getCallerId().matches(getStartsWithRegex(type.toString()));
  }

  public double getFileSizeMb() {
    return (double) getFileSize() / (1024 * 1024);
  }

  @RequiredArgsConstructor
  public static class FileSizeComparator implements Comparator<FileMetaData> {

    private final boolean ascending;

    @Override
    public int compare(FileMetaData f1, FileMetaData f2) {
      if (ascending) {
        return Long.compare(f1.getFileSize(), f2.getFileSize());
      } else {
        return Long.compare(f2.getFileSize(), f1.getFileSize());
      }
    }
  }

  @RequiredArgsConstructor
  public static class FilenameComparator implements Comparator<FileMetaData> {

    @Override
    public int compare(FileMetaData f1, FileMetaData f2) {
      return f1.getVcfFilenameParser().getFilename().compareTo(f2.getVcfFilenameParser().getFilename());
    }
  }

}
