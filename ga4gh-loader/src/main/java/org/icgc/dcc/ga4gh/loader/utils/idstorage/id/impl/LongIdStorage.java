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

package org.icgc.dcc.ga4gh.loader.utils.idstorage.id.impl;

import org.icgc.dcc.ga4gh.loader.utils.idstorage.id.AbstractIdStorageTemplate;
import org.icgc.dcc.ga4gh.loader.utils.idstorage.storage.MapStorage;

import static com.google.common.base.Preconditions.checkState;

public class LongIdStorage<K> extends AbstractIdStorageTemplate<K, Long> {

  private static final Long SINGLE_INCREMENT_AMOUNT = 1L;

  private LongIdStorage( MapStorage<K, Long> objectCentricMapStorage, Long initCount) {
    super(objectCentricMapStorage, initCount);
  }

  @Override
  protected void checkIdLowerBound() {
    final Long count = getCount();
    checkState(count >= Long.MIN_VALUE, "The id %d must be >= %d", count, Long.MIN_VALUE);
  }

  @Override
  protected void checkIdUpperBound() {
    final Long count = getCount();
    checkState(count < Long.MAX_VALUE, "The id %d must be < %d", count, Long.MAX_VALUE);

  }

  @Override
  protected Long incr() {
    final Long out = getCount();
    setCount(getCount() + SINGLE_INCREMENT_AMOUNT);
    return out;
  }

  public static <K> LongIdStorage<K> createLongIdStorage(
      MapStorage<K, Long> objectCentricMapStorage, Long initCount) {
    return new LongIdStorage<K>(objectCentricMapStorage, initCount);
  }

}
