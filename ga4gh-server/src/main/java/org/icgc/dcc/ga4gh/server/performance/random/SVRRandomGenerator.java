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

import com.google.common.collect.Sets;
import ga4gh.VariantServiceOuterClass.SearchVariantsRequest;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.Random;

@Slf4j
@RequiredArgsConstructor
public class SVRRandomGenerator implements RandomGenerator<SearchVariantsRequest> {


  private final RandomGenerator<Integer> startGenerator;
  private final RandomGenerator<Integer> variantSetIdGenerator;
  private final RandomGenerator<Integer> callSetIdGenerator;
  private final RandomGenerator<String> referenceNameGenerator;
  @Getter private final int variantLength;
  private final int pageSize;
  private static final int MAX_NUM_CALLSET_IDS = 5;

  @Override
  public SearchVariantsRequest nextRandom(Random random){
    val numberOfCallSetIds = random.nextInt(MAX_NUM_CALLSET_IDS) + 1;
    val callSetIds = Sets.<String>newHashSet();

    for (int i = 0; i < numberOfCallSetIds; i++) {
      callSetIds.add(callSetIdGenerator.nextRandom(random).toString());
    }
    val start = startGenerator.nextRandom(random);
    return SearchVariantsRequest.newBuilder()
        .setReferenceName(referenceNameGenerator.nextRandom(random))
        .setStart(start)
        .setEnd(start + variantLength)
        .setVariantSetId(variantSetIdGenerator.nextRandom(random).toString())
        .setPageSize(pageSize)
        .setPageToken("")
        .build();
  }

  public static SVRRandomGenerator createSVRRandomGenerator(RandomGenerator<Integer> startGenerator,
      RandomGenerator<Integer> variantSetIdGenerator,
      RandomGenerator<Integer> callSetIdGenerator,
      RandomGenerator<String> referenceNameGenerator, int variantLength, int pageSize) {
    return new SVRRandomGenerator(startGenerator, variantSetIdGenerator, callSetIdGenerator, referenceNameGenerator,
        variantLength, pageSize);
  }

}
