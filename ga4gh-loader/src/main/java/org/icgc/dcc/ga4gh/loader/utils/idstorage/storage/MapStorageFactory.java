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

package org.icgc.dcc.ga4gh.loader.utils.idstorage.storage;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import org.icgc.dcc.ga4gh.loader.utils.idstorage.storage.impl.DirectMemoryMapStorage;
import org.icgc.dcc.ga4gh.loader.utils.idstorage.storage.impl.DiskMapStorage;
import org.icgc.dcc.ga4gh.loader.utils.idstorage.storage.impl.RamMapStorage;
import org.mapdb.Serializer;

import java.nio.file.Path;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static org.icgc.dcc.ga4gh.loader.utils.CheckPaths.checkFilePath;
import static org.icgc.dcc.ga4gh.loader.utils.idstorage.storage.impl.DiskMapStorage.generateFilepath;
import static org.icgc.dcc.ga4gh.loader.utils.idstorage.storage.impl.DiskMapStorage.newDiskMapStorage;
import static org.icgc.dcc.ga4gh.loader.utils.idstorage.storage.impl.RamMapStorage.newRamMapStorage;

@RequiredArgsConstructor
public class MapStorageFactory<K, V> {

  @NonNull  private final String name;
  @NonNull private final Serializer<K> keySerializer;
  @NonNull private final Serializer<V> valueSerializer;
  @NonNull private final Path outputDir;
  private final long allocation;

  public Path getPath(){
    return generateFilepath(name, outputDir);
  }

  @SneakyThrows
  public DirectMemoryMapStorage<K,V> createDirectMemoryMapStorage(boolean persistFile){
    val dms = createDiskMapStorage(persistFile);
    return DirectMemoryMapStorage.createDirectMemoryMapStorage(name, keySerializer,valueSerializer,dms);
  }

  @SneakyThrows
  public DiskMapStorage<K, V> createDiskMapStorage(boolean persistFile){
    return newDiskMapStorage(name, keySerializer, valueSerializer,outputDir,allocation,persistFile);
  }

  public RamMapStorage<K, V> createRamMapStorage(){
    return newRamMapStorage();
  }

  public MapStorage<K,V> createMapStorage(boolean useDisk, boolean persistFile){
    if (persistFile){
      checkFilePath(getPath());
    }
    return useDisk ? createDiskMapStorage(persistFile) : createRamMapStorage();
  }

  public MapStorage<K,V> createNewMapStorage(boolean useDisk){
    return createMapStorage(useDisk, FALSE);
  }

  public MapStorage<K,V> persistMapStorage(){
    return createMapStorage(TRUE, TRUE);
  }

  public static <K, V> MapStorageFactory<K, V> createMapStorageFactory(String name, Serializer<K> keySerializer,
      Serializer<V> valueSerializer,
      Path outputDir, final long allocation) {
    return new MapStorageFactory<K, V>(name, keySerializer, valueSerializer, outputDir, allocation);
  }

}
