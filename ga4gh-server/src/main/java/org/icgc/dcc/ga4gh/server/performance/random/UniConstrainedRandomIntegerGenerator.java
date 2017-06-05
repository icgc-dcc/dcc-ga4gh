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

package org.icgc.dcc.ga4gh.server.performance.random;

import lombok.Getter;

import java.util.Random;

import static com.google.common.base.Preconditions.checkArgument;

public class UniConstrainedRandomIntegerGenerator implements RandomGenerator<Integer> {

  public static UniConstrainedRandomIntegerGenerator createUniConstrainedRandomIntegerGenerator(int min,
      int max) {
    return new UniConstrainedRandomIntegerGenerator(min, max);
  }

  private final int min;
  @Getter private final int range;

  private UniConstrainedRandomIntegerGenerator(int min, int max) {
    checkArgument(min <= max && min >= 0, "The min must be <= max, and min must be >= 0");
    this.min = min; // inclusive
    this.range = max - min;
  }

  public Integer nextRandom(Random random) {
    return min + random.nextInt(range);
  }

}
