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

package org.icgc.dcc.ga4gh.loader.utils;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import java.nio.file.Path;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;
import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.ga4gh.loader.utils.CheckPaths.checkDirPath;

@RequiredArgsConstructor(access = PRIVATE)
public class MapFactory<K,V> {

  @NonNull private final String name;
  @NonNull private final Path dirname;
  @NonNull private final Serializer<K> keySerializer;
  @NonNull private final Serializer<V> valueSerializer;
  private final long allocation;


  public Path getFilename(){
    return dirname.resolve(name+".db");
  }

  public Map<K,V> createRamMap(){
    return newHashMap();
  }

  public  Map<K, V> createConcurrentMemoryMap() {
    val diskMap = createDiskMap();
      return DBMaker
          .memoryShardedHashMap(8)
          .valueSerializer(valueSerializer)
          .keySerializer(keySerializer)
          .layout(8, 128, 490000)
          .expireOverflow(diskMap)
          .createOrOpen();
  }

  public Map<K, V> createDiskMap() {
    return DBMaker
            .fileDB(getFilename().toString())
            .concurrencyDisable()
            .closeOnJvmShutdown()
            .allocateIncrement(allocation) //TODO: rtisma_20170511_hack
            .allocateStartSize(allocation) //TODO: rtisma_20170511_hack
          .make()
        .hashMap(name, keySerializer, valueSerializer)
        .createOrOpen();
  }

  public static <K, V> MapFactory<K, V> createMapDBFactory(String name, Path dirname, Serializer<K> keySerializer,
      Serializer<V> valueSerializer,
      long allocation) {
    checkDirPath(dirname);
    return new MapFactory<K, V>(name, dirname, keySerializer, valueSerializer, allocation);
  }

}
