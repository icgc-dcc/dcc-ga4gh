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

import static java.util.stream.Collectors.groupingBy;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;
import static org.icgc.dcc.common.core.util.stream.Streams.stream;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

import org.collaboratory.ga4gh.loader.Benchmarks;
import org.collaboratory.ga4gh.loader.PortalFiles;
import org.collaboratory.ga4gh.loader.PortalVCFFilenameParser;
import org.collaboratory.ga4gh.loader.enums.CallerTypes;
import org.collaboratory.ga4gh.loader.enums.MutationTypes;
import org.collaboratory.ga4gh.loader.enums.SubMutationTypes;
import org.icgc.dcc.common.core.util.Joiners;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

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

  public static List<FileMetaData> sortByFileSize(@NonNull final Iterable<FileMetaData> fileMetaDatas,
      final boolean ascending) {
    val list = Lists.newArrayList(fileMetaDatas);
    Collections.sort(list, new FileSizeComparator(ascending));
    return ImmutableList.copyOf(list);
  }

  @RequiredArgsConstructor
  private static class FileSizeComparator implements Comparator<FileMetaData> {

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

  public double getFileSizeMb() {
    return (double) getFileSize() / (1024 * 1024);
  }

  @SuppressWarnings("unchecked")
  public static List<FileMetaData> restore(final String filename) {
    FileInputStream fin = null;
    ObjectInputStream ois = null;
    try {
      fin = new FileInputStream(filename);
      ois = new ObjectInputStream(fin);
      ImmutableList<FileMetaData> fileMetaDatas = (ImmutableList<FileMetaData>) ois.readObject();
      log.info("Restored FileMetaDatas from {}", filename);
      return fileMetaDatas;
    } catch (Exception e) {
      log.error("[{}] - Message - {}:\n{}", e.getClass().getName(), e.getMessage(), e);
    } finally {
      if (fin != null) {
        try {
          fin.close();
        } catch (IOException e) {
          log.error("[{}] - Message - {}:\n{}", e.getClass().getName(), e.getMessage(), e);
        }
      }

      if (ois != null) {
        try {
          ois.close();
        } catch (IOException e) {
          log.error("[{}] - Message - {}:\n{}", e.getClass().getName(), e.getMessage(), e);
        }
      }
    }
    return null;
  }

  public static void store(final Iterable<FileMetaData> fileMetaDatas, final String filename) {
    FileOutputStream fout = null;
    ObjectOutputStream oos = null;
    try {
      fout = new FileOutputStream(filename);
      oos = new ObjectOutputStream(fout);
      ImmutableList<FileMetaData> list = ImmutableList.copyOf(fileMetaDatas);
      oos.writeObject(list);
      log.info("Saved FileMetaDatas to {}", filename);
    } catch (Exception e) {
      log.error("[{}] - Message - {}:\n{}", e.getClass().getName(), e.getMessage(), e);
    } finally {

      if (fout != null) {
        try {
          fout.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }

      if (oos != null) {
        try {
          oos.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

}