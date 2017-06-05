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

package org.icgc.dcc.ga4gh.server.performance.sweep;

import ga4gh.VariantServiceOuterClass.SearchVariantsRequest;
import lombok.NonNull;
import lombok.val;
import org.eclipse.collections.impl.factory.Stacks;

import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;

public class SVRSweeperFlattener implements Iterator<SearchVariantsRequest.Builder> {

  @NonNull private final List<SVRSweeper> list;

  private SVRSweeper currentSubIterator;
  private Iterator<SVRSweeper> listIterator;

  public boolean hasNext2() {
    if (currentSubIterator.hasNext()){
      return true;
    } else if(listIterator.hasNext()) {
      currentSubIterator = listIterator.next();
      return hasNext();
    }
    return false;
  }

  public boolean hasNext(){
    val stack = Stacks.mutable.of(currentSubIterator);
    boolean result = false;
    while(stack.size() > 0){
      val cc = stack.pop();
      if (cc.hasNext()){
        result = true;
      } else if (listIterator.hasNext()){
        currentSubIterator = listIterator.next();
        stack.push(currentSubIterator);
      }
    }
    return result;
  }

  public int getSize(){
    int result = 0;
    for (val sub : list){
      result+= sub.getSize();
    }
    return result;
  }

  @Override public SearchVariantsRequest.Builder next() {
    return currentSubIterator.next();
  }

  private SVRSweeperFlattener(
      List<SVRSweeper> list) {
    this.list = list;
    checkState(!list.isEmpty(), "the list is empty");
    listIterator = list.iterator();
    currentSubIterator = listIterator.next();
  }

  public static SVRSweeperFlattener createSVRSweeperFlattener(
      List<SVRSweeper> list) {
    return new SVRSweeperFlattener(list);
  }
}
