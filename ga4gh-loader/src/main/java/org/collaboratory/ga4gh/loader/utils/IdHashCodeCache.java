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

import java.util.Map;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/*
 * Decorator for IdCache. Basically use IdObjectCache<Integer> instance, and converts the object 
 * passed to each member method to a hashCode (integer) and then that interacts with IdObjectCache<Integer>
 */
@RequiredArgsConstructor
public class IdHashCodeCache<T> implements IdCache<T> {

  public static <T> IdCache<T> newIdCache(Map<Integer, Long> cache, final long init_id) {
    return new IdHashCodeCache<T>(IdCacheImpl.<Integer> newIdCache(cache, init_id));
  }

  @NonNull
  private final IdCache<Integer> idCache;

  @Override
  public void add(T t) {
    idCache.add(t.hashCode());
  }

  @Override
  public boolean contains(T t) {
    return idCache.contains(t.hashCode());
  }

  @Override
  public String getIdAsString(T t) {
    return idCache.getIdAsString(t.hashCode());
  }

  @Override
  public Long getId(T t) {
    return idCache.getId(t.hashCode());
  }

}
