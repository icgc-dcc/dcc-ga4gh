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

package org.icgc.dcc.ga4gh.loader.utils.counting;

import lombok.val;

public class LongCounter implements Counter<Long> {

  private final long init;

  private long count;

  public LongCounter(long init) {
    this.init = init;
    this.count = init;
  }

  @Override
  public Long preIncr() {
    return ++count;
  }

  @Override
  public Long preIncr(Long amount) {
    count += amount;
    return count;
  }

  @Override
  public void reset() {
    count = init;
  }

  @Override
  public Long getCount() {
    return count;
  }

  @Override
  public Long postIncr() {
    return count++;
  }

  @Override
  public Long postIncr(Long amount) {
    val post = count;
    count += amount;
    return post;
  }

  public static LongCounter createLongCounter0() {
    return createLongCounter(0L);
  }

  public static LongCounter createLongCounter(long init) {
    return new LongCounter(init);
  }

}
