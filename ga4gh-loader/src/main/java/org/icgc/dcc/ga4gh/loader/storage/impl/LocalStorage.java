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

package org.icgc.dcc.ga4gh.loader.storage.impl;

import com.google.common.collect.ImmutableMap;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.ga4gh.common.model.portal.PortalFilename;
import org.icgc.dcc.ga4gh.common.model.portal.PortalMetadata;
import org.icgc.dcc.ga4gh.loader.storage.LocalStorageFileNotFoundException;
import org.icgc.dcc.ga4gh.loader.storage.Storage;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.nio.file.FileVisitResult.CONTINUE;
import static java.nio.file.FileVisitResult.SKIP_SUBTREE;
import static lombok.AccessLevel.PRIVATE;

@RequiredArgsConstructor(access = PRIVATE)
@Slf4j
public class LocalStorage implements Storage {

  @NonNull private final Map<PortalFilename, Path> fileMap;

  private final boolean bypassMd5Check;

  @Override
  @SneakyThrows
  public File getFile(PortalMetadata portalMetadata) {
    val portalFilename = portalMetadata.getPortalFilename();
    val found = fileMap.containsKey(portalFilename);
    if(found){
      val file = fileMap.get(portalFilename);
      if (bypassMd5Check){
        return file.toFile();
      } else {
        val sum = Storage.calcMd5Sum(file);
        checkState(sum.equals(portalMetadata.getFileMd5sum()),
            "The local file [%s] MD5CheckSum[%s], expecting[%s] from portal",
            file.getFileName(), sum, portalMetadata.getFileMd5sum());
          return file.toFile();
      }
    } else {
      val message = String.format("Cannot find the file [%s]", portalFilename);
      throw new LocalStorageFileNotFoundException(message, portalMetadata);
    }
  }

  @SneakyThrows
  private static Map<PortalFilename, Path> createFileMap(Path dirpath){
    checkArgument(dirpath.toFile().exists(), "Directory [%s] DNE", dirpath.toString());
    val visitor = new CurrentDirectoryVcfFileVisitor(dirpath);
    Files.walkFileTree(dirpath , visitor);
    return visitor.getPathMap();
  }

  public static LocalStorage newLocalStorage(Path inputDir, final boolean bypassMd5Check){
    return new LocalStorage(createFileMap(inputDir), bypassMd5Check);
  }

  @Slf4j
  @RequiredArgsConstructor
  public static class CurrentDirectoryVcfFileVisitor extends SimpleFileVisitor<Path> {


    @NonNull private final Path inputDir;
    private final ImmutableMap.Builder<PortalFilename, Path> map = ImmutableMap.<PortalFilename, Path>builder();

    // Only analyze files in current directory, becuase want unique file names
    @Override public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
      return dir.compareTo(inputDir) == 0 ? CONTINUE : SKIP_SUBTREE;
    }

    @Override public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
      val filename = file.getFileName().toString();
      try{
        val portalFilename = PortalFilename.createPortalFilename(filename);
        map.put(portalFilename, file);
      } catch (Exception e){
        log.error("Error parsing Portal VCF Filename: {}", file.getFileName());
      }
      return CONTINUE;
    }

    public Map<PortalFilename, Path> getPathMap(){
      return map.build();
    }

  }

}

