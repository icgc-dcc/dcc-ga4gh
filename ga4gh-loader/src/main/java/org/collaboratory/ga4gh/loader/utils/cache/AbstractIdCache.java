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
package org.collaboratory.ga4gh.loader.utils.cache;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.val;

public abstract class AbstractIdCache<K, ID extends Number> implements IdCache<K, ID> {

  private Map<K, ID> cache;
  private CacheStorage<K, ID> cacheStorage;

  @Getter(AccessLevel.PROTECTED)
  @Setter(AccessLevel.PROTECTED)
  private ID count;

  public AbstractIdCache(@NonNull final CacheStorage<K, ID> cacheStorage, final ID initCount) {
    this.count = initCount;
    this.cacheStorage = cacheStorage;
    this.cache = cacheStorage.getMap();

    this.checkIdLowerBound();
    this.checkIdUpperBound();
  }

  protected abstract void checkIdLowerBound();

  protected abstract void checkIdUpperBound();

  protected abstract ID incr();

  @Override
  public void add(final K k) {
    checkIdUpperBound(); // Assume always increasing ids, and passed checkIdLowerBound in constructor
    if (!cache.containsKey(k)) {
      cache.put(k, incr());
    }
  }

  @Override
  public boolean contains(final K k) {
    return cache.containsKey(k);
  }

  @Override
  public String getIdAsString(@NonNull K k) {
    return getId(k).toString();
  }

  @Override
  public ID getId(@NonNull K k) {
    checkArgument(cache.containsKey(k), "The following key doesnt not exist in the cache: \n%s", k);
    return cache.get(k);
  }

  @Override
  public Map<ID, K> getReverseCache() {
    val map = ImmutableMap.<ID, K> builder();
    for (val entry : cache.entrySet()) {
      val key = entry.getKey();
      val idValue = entry.getValue();
      map.put(idValue, key);
    }
    return map.build();
  }

  @Override
  public void purge() {
    cacheStorage.purge();
  }

}
