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
package org.icgc.dcc.ga4gh.loader2.utils.idstorage.id;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.val;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.storage.MapStorage;

import java.util.Map;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;

public abstract class AbstractIdStorageTemplate<K, ID extends Number> implements IdStorage<K, ID> {

  private Map<K, ID> objectCentricCache;
  private MapStorage<K, ID> objectCentricMapStorage;

  @Getter(AccessLevel.PROTECTED)
  @Setter(AccessLevel.PROTECTED)
  private ID count;

  public AbstractIdStorageTemplate(@NonNull final MapStorage<K, ID> objectCentricMapStorage, final ID initCount) {
    this.count = initCount;
    this.objectCentricMapStorage = objectCentricMapStorage;
    this.objectCentricCache = objectCentricMapStorage.getMap();

    this.checkIdLowerBound();
    this.checkIdUpperBound();
  }

  protected abstract void checkIdLowerBound();

  protected abstract void checkIdUpperBound();

  protected abstract ID incr();

  @Override
  public void add(final K k) {
    checkIdUpperBound(); // Assume always increasing ids, and passed checkIdLowerBound in constructor
    if (!containsObject(k)) {
      val i = incr();
      objectCentricCache.put(k, i);
    }
  }

  @Override
  public boolean containsObject(final K k) {
    return objectCentricCache.containsKey(k);
  }

  @Override
  public String getIdAsString(@NonNull K k) {
    return getId(k).toString();
  }

  @Override
  public ID getId(@NonNull K k) {
    checkArgument(objectCentricCache.containsKey(k), "The following key doesnt not exist in the idstorage: \n%s", k);
    return objectCentricCache.get(k);
  }

  @Override
  public void purge() {
    objectCentricMapStorage.purge();
  }

  @Override
  public Stream<Map.Entry<K, ID>> streamEntries(){
    return objectCentricCache.entrySet().stream();
  }

}