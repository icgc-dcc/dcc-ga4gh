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

public class IntegerIdStorage<K> extends AbstractIdStorageTemplate<K, Integer> {

  private static final Integer SINGLE_INCREMENT_AMOUNT = 1;

  private IntegerIdStorage(
      MapStorage<K, Integer> objectCentricMapStorage, Integer initCount) {
    super(objectCentricMapStorage, initCount);
  }

  @Override
  protected void checkIdLowerBound() {
    final Integer count = getCount();
    checkState(count >= Integer.MIN_VALUE, "The id %d must be >= %d", count, Integer.MIN_VALUE);
  }

  @Override
  protected void checkIdUpperBound() {
    final Integer count = getCount();
    checkState(count < Integer.MAX_VALUE, "The id %d must be < %d", count, Integer.MAX_VALUE);

  }

  @Override
  protected Integer incr() {
    final Integer out = getCount();
    setCount(getCount() + SINGLE_INCREMENT_AMOUNT);
    return out;
  }

  public static <K> IntegerIdStorage<K> createIntegerIdStorage(
      MapStorage<K, Integer> objectCentricMapStorage, Integer initCount) {
    return new IntegerIdStorage<K>(objectCentricMapStorage, initCount);
  }

}
