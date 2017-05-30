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

package org.icgc.dcc.ga4gh.loader.utils.idstorage.storage.impl;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.icgc.dcc.ga4gh.loader.utils.idstorage.storage.MapStorage;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import java.io.IOException;
import java.util.Map;

@Slf4j
public class DirectMemoryMapStorage<K,V> implements MapStorage<K,V>{

  @NonNull private final String name;
  @NonNull private final MapStorage<K,V> diskMapStorage;

  private DB db;
  private Map<K,V> memoryMap;

  private DirectMemoryMapStorage(String name, Serializer<K> keySerializer, Serializer<V> valueSerializer,
      MapStorage<K, V> diskMapStorage) {
    this.name = name;
    this.diskMapStorage = diskMapStorage;
    this.db = createDirectMemoryDB();
    this.memoryMap = createDirectMemoryMap(db, name, keySerializer, valueSerializer, diskMapStorage.getMap());
  }


  //create direct memory map storage, and use diskMapStorage as overflow
  @Override public Map<K, V> getMap() {
    return memoryMap;
  }

  private static DB createDirectMemoryDB(){
    return DBMaker
        .memoryDirectDB()
        .closeOnJvmShutdown()
        .concurrencyDisable()
        .make();
  }

  private static <K,V> Map<K,V> createDirectMemoryMap(DB db, String name, Serializer<K> keySerializer, Serializer<V> valueSerializer,  Map<K,V> diskMap ){
    return db
            .hashMap(name, keySerializer, valueSerializer )
            .expireOverflow(diskMap)
            .createOrOpen();
  }

  @Override
  public void close() throws IOException {
    db.close();
    this.diskMapStorage.close();
    log.info("Closed [{}] [{}]", this.name, this.getClass().getSimpleName());
  }

  @Override
  public void purge() {
    try {
      close();
      this.diskMapStorage.purge();
    } catch (IOException e) {
      log.error("Was not able to purge MapStorage: name: {}",name );
    }
  }

  public static <K, V> DirectMemoryMapStorage<K, V> createDirectMemoryMapStorage(String name,
      Serializer<K> keySerializer, Serializer<V> valueSerializer,
      MapStorage<K, V> diskMapStorage) {
    return new DirectMemoryMapStorage<K, V>(name, keySerializer, valueSerializer, diskMapStorage);
  }

}
