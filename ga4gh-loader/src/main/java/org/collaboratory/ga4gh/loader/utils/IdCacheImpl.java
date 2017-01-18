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
package org.collaboratory.ga4gh.loader.utils;

import static com.google.common.base.Preconditions.checkState;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

import lombok.NonNull;
import lombok.val;

public class IdCacheImpl<T> implements IdCache<T> {

  public static <T> IdCache<T> newIdCache(Map<T, Long> cache, final Long id) {
    return new IdCacheImpl<T>(cache, id);
  }

  private final Map<T, Long> cache;

  private Long id;

  public IdCacheImpl(@NonNull final Map<T, Long> cache, final Long id) {
    this.id = id;
    this.checkIdLowerBound();
    this.checkIdUpperBound();
    this.cache = cache;
  }

  private void checkIdLowerBound() {
    checkState(id >= Long.MIN_VALUE, "The id %d must be >= %d", id, Long.MIN_VALUE);
  }

  private void checkIdUpperBound() {
    checkState(id < Long.MAX_VALUE, "The id %d must be < %d", id, Long.MAX_VALUE);
  }

  @Override
  public void add(final T t) {
    checkIdUpperBound(); // Assume always increasing ids, and passed checkIdLowerBound in constructor
    cache.put(t, id++);
  }

  @Override
  public boolean contains(final T t) {
    return cache.containsKey(t);
  }

  @Override
  public String getIdAsString(@NonNull T t) {
    return getId(t).toString();
  }

  @Override
  public Long getId(@NonNull T t) {
    if (cache.containsKey(t)) {
      return cache.get(t);
    } else {
      throw new NullPointerException("The following key doesnt not exist in the cache: \n" + t);
    }
  }

  public Map<Long, T> getReverseCache() {
    val map = ImmutableMap.<Long, T> builder();
    for (val entry : cache.entrySet()) {
      val key = entry.getKey();
      val idValue = entry.getValue();
      map.put(idValue, key);
    }
    return map.build();
  }
}
