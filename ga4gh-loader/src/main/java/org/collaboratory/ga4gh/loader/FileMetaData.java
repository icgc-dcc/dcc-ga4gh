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
package org.collaboratory.ga4gh.loader;

import static java.util.stream.Collectors.groupingBy;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;
import static org.icgc.dcc.common.core.util.stream.Streams.stream;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

import org.collaboratory.ga4gh.loader.enums.CallerTypes;
import org.collaboratory.ga4gh.loader.enums.MutationTypes;
import org.collaboratory.ga4gh.loader.enums.SubMutationTypes;
import org.icgc.dcc.common.core.util.Joiners;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;

import lombok.Data;
import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

//TODO: [rtisma] -- consider storing the CallerTypes, MutationTypes and MutationSubTypes enum values instead of string representation. Or atleast keep strings, just create functions to compare the string against the enum
@Slf4j
@Data
public final class FileMetaData {

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

  /*
   * State
   */
  private boolean corrupted = false;

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

  public static List<FileMetaData> filter(@NonNull final Iterable<FileMetaData> fileMetaDatas,
      @NonNull final Predicate<? super FileMetaData> predicate) {
    return stream(fileMetaDatas).filter(predicate).collect(toImmutableList());
  }

  public static List<FileMetaData> buildFileMetaDataList(@NonNull final Iterable<ObjectNode> objectNodes) {
    return stream(objectNodes).map(FileMetaData::buildFileMetaData).collect(toImmutableList());
  }

  public static Map<String, List<FileMetaData>> groupFileMetaDataBySample(
      @NonNull final Iterable<FileMetaData> fileMetaDatas) {
    return groupFileMetaData(fileMetaDatas, FileMetaData::getSampleId);
  }

  public static Map<String, List<FileMetaData>> groupFileMetaDatasByDonor(
      @NonNull final Iterable<FileMetaData> fileMetaDatas) {
    return groupFileMetaData(fileMetaDatas, FileMetaData::getDonorId);
  }

  public static Map<String, List<FileMetaData>> groupFileMetaDatasByDataType(
      @NonNull final Iterable<FileMetaData> fileMetaDatas) {
    return groupFileMetaData(fileMetaDatas, FileMetaData::getDataType);
  }

  public static Map<String, List<FileMetaData>> groupFileMetaDatasByMutationType(
      @NonNull final Iterable<FileMetaData> fileMetaDatas) {
    return groupFileMetaData(fileMetaDatas, x -> x.getVcfFilenameParser().getMutationType());
  }

  public static Map<String, List<FileMetaData>> groupFileMetaDatasBySubMutationType(
      @NonNull final Iterable<FileMetaData> fileMetaDatas) {
    return groupFileMetaData(fileMetaDatas, x -> x.getVcfFilenameParser().getSubMutationType());
  }

  public static Map<String, List<FileMetaData>> groupFileMetaData(
      @NonNull final Iterable<FileMetaData> fileMetaDatas,
      final Function<? super FileMetaData, ? extends String> functor) {
    return ImmutableMap.copyOf(stream(fileMetaDatas).collect(groupingBy(functor, toImmutableList())));
  }

  public static void writeStats(final String outputFn,
      @NonNull final List<FileMetaData> fileMetaList) {
    val sb = new StringBuilder();
    fileMetaList.stream()
        .collect(
            groupingBy(
                x -> Arrays.asList(x.getDonorId(),
                    x.getSampleId(),
                    x.getVcfFilenameParser().getCallerId())))
        .entrySet()
        .stream().filter(x -> x.getValue().size() > 1)
        .forEach(e -> sb.append(
            Joiners.COMMA.join(e.getKey())
                + "," + e.getValue().size() + "\t--[\n\t\t"
                + Joiner.on(",\n\t\t").join(e.getValue().stream()
                    .map(y -> y.getVcfFilenameParser().getFilename()).toArray())
                + "]\n\n"));

    Benchmarks.writeToNewFile(outputFn, sb.toString());
    log.info("sdfsdf");
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

}
