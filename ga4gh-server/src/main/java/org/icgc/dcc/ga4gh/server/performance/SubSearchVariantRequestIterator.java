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

package org.icgc.dcc.ga4gh.server.performance;

import ga4gh.VariantServiceOuterClass;
import lombok.NonNull;
import lombok.val;

import java.util.Iterator;

public class SubSearchVariantRequestIterator
    implements Iterator<VariantServiceOuterClass.SearchVariantsRequest.Builder> {

  public static SubSearchVariantRequestIterator createSubSearchVariantRequestIterator(
      String referenceName, int minStart, int maxEnd, int variantLength) {
    return new SubSearchVariantRequestIterator(referenceName, minStart, maxEnd,
        variantLength);
  }

  @NonNull private final String referenceName;
  private final int minStart;
  private final int maxEnd;
  private final int variantLength;


  private int currentStart;

  public SubSearchVariantRequestIterator(String referenceName, int minStart, int maxEnd, int variantLength) {
    this.referenceName = referenceName;
    this.minStart = minStart;
    this.maxEnd = maxEnd;
    this.variantLength = variantLength;
    this.currentStart = minStart;
  }

  public int getSize(){
    return (int)Math.floor((maxEnd - variantLength - minStart)/(double)variantLength);
  }


  @Override public boolean hasNext() {
    return currentStart + variantLength < maxEnd;
  }

  @Override public VariantServiceOuterClass.SearchVariantsRequest.Builder next() {
    val builder = VariantServiceOuterClass.SearchVariantsRequest.newBuilder()
        .setReferenceName(referenceName)
        .setEnd(currentStart + variantLength)
        .setStart(currentStart);
    currentStart += variantLength;
    return builder;
  }

}
