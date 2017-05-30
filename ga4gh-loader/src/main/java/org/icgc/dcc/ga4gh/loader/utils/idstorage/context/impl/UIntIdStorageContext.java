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

package org.icgc.dcc.ga4gh.loader.utils.idstorage.context.impl;

import org.icgc.dcc.ga4gh.loader.utils.idstorage.context.IdStorageContext;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Lists.newArrayList;

public class UIntIdStorageContext<T> implements IdStorageContext<Long,T> {

  private static final long MAX_UINT = (1L << 32) - 1;
  private static final long MIN_UINT = 0L;

  private final int uintId;
  private final List<T> objects;

  public UIntIdStorageContext(long id, List<T> objects) {
    checkArgument(id <= MAX_UINT && id >= MIN_UINT,  "The input ID [%s] must be uint, or between %s and %s", id, MIN_UINT, MAX_UINT);
    this.uintId = (int)(id + Integer.MIN_VALUE); // convertBasic to uint integer
    this.objects = objects;
  }

  public UIntIdStorageContext(long id) {
    this(id, newArrayList());
  }

  @Override public void add(T object) {
    objects.add(object);
  }

  @Override public void addAll(List<T> objects) {
    this.objects.addAll(objects);
  }

  @Override public Long getId() {
    return (long)uintId - Integer.MIN_VALUE ; // convertBasic back to long
  }

  @Override public List<T> getObjects() {
    return objects;
  }

  public static <T> UIntIdStorageContext<T> createUIntIdStorageContext(long id ){
    return new UIntIdStorageContext<T>(id);
  }

  public static <T> UIntIdStorageContext<T> createUIntIdStorageContext(long id, List<T> objects ){
    return new UIntIdStorageContext<T>(id, objects);
  }

}
