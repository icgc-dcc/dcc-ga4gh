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

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;

import lombok.NonNull;
import lombok.Value;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Value
@Slf4j
public final class FileMetaData {

  @NonNull
  public final String objectId;

  @NonNull
  public final String fileId;

  @NonNull
  public final String sampleId;

  @NonNull
  public final String donorId;

  @NonNull
  public final String dataType;

  @NonNull
  public final String referenceName;

  @NonNull
  public final String genomeBuild;

  public final long fileSize;

  @NonNull
  public final String fileMd5sum;

  @NonNull
  public final PortalVCFFilenameParser vcfFilenameParser;

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
            Joiner.on(",").join(e.getKey())
                + "," + e.getValue().size() + "\t--[\n\t\t"
                + Joiner.on(",\n\t\t").join(e.getValue().stream()
                    .map(y -> y.getVcfFilenameParser().getFilename()).toArray())
                + "]\n\n"));

    Benchmarks.writeToNewFile(outputFn, sb.toString());
    log.info("sdfsdf");
  }

}
