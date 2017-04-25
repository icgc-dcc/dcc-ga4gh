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
package org.icgc.dcc.ga4gh.loader.factory.idcache;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.icgc.dcc.ga4gh.common.model.es.EsVariant;
import org.icgc.dcc.ga4gh.loader.utils.cache.storage.CacheStorage;
import org.icgc.dcc.ga4gh.loader.utils.cache.id.IdCache;
import org.icgc.dcc.ga4gh.loader.utils.cache.id.impl.IntegerIdCache;
import org.icgc.dcc.ga4gh.loader.utils.cache.id.impl.LongIdCache;

import java.io.IOException;

import static lombok.AccessLevel.PROTECTED;

@RequiredArgsConstructor
public abstract class AbstractIdCacheFactoryTemplate implements IdCacheFactory {

  private final int initId;

  @Getter
  private IdCache<EsVariant, Long> variantIdCache;

  @Getter
  private IdCache<String, Integer> variantSetIdCache;

  @Getter
  private IdCache<String, Integer> callSetIdCache;

  @Setter(PROTECTED)
  private CacheStorage<EsVariant, Long> variantCacheStorage;

  @Setter(PROTECTED)
  private CacheStorage<String, Integer> variantSetCacheStorage;

  @Setter(PROTECTED)
  private CacheStorage<String, Integer> callSetCacheStorage;

  @Override
  public final void build() throws IOException {
    buildCacheStorage();
    variantIdCache = LongIdCache.newLongIdCache(variantCacheStorage, (long) initId);
    variantSetIdCache = IntegerIdCache.newIntegerIdCache(variantSetCacheStorage, initId);
    callSetIdCache = IntegerIdCache.newIntegerIdCache(callSetCacheStorage, initId);
  }

  @Override
  public final void purge() {
    variantSetIdCache.purge();
    variantIdCache.purge();
    callSetIdCache.purge();
  }

  @Override
  public final void close() throws IOException{
    variantCacheStorage.close();
    variantSetCacheStorage.close();
    callSetCacheStorage.close();
  }

  protected abstract void buildCacheStorage() throws IOException;

}