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

import static com.google.common.collect.Maps.newHashMap;
import static org.collaboratory.ga4gh.loader.utils.IdCacheImpl.newIdCache;

import java.util.Map;

public class IdRamCache<T> implements IdCache<T> {

  private final IdCache<T> idCache;

  public static <T> IdRamCache<T> newIdRamCache(final Long id) {
    return new IdRamCache<T>(id);
  }

  public IdRamCache(final long initId) {
    this.idCache = newIdCache(newHashMap(), initId);
  }

  @Override
  public void purge() {
    idCache.purge();
  }

  @Override
  public void add(T t) {
    idCache.add(t);
  }

  @Override
  public boolean contains(T t) {
    return idCache.contains(t);
  }

  @Override
  public String getIdAsString(T t) {
    return idCache.getIdAsString(t);
  }

  @Override
  public Long getId(T t) {
    return idCache.getId(t);
  }

  @Override
  public Map<Long, T> getReverseCache() {
    return idCache.getReverseCache();
  }

}
