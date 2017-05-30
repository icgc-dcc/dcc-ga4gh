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

package org.icgc.dcc.ga4gh.common;

import com.google.common.collect.ImmutableList;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;

import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

import static lombok.AccessLevel.PRIVATE;

@RequiredArgsConstructor
public enum CompareState {
  LT(i -> i < 0, -1),
  EQ(i -> i == 0, 0),
  GT(i -> i > 0, 1);

  @Getter(PRIVATE)
  private final Predicate<Integer> predicate;

  @Getter
  private final int value;

  public static <T extends Comparable<T>> CompareState getState(T t1, T t2){
    return getState(t1.compareTo(t2));
  }

  public static <T> CompareState getState(Comparator<T>  comparator,  T t1, T t2){
    return getState(comparator.compare(t1, t2));
  }

  public static CompareState getState(int i){
    for (val state : values()){
      if (state.getPredicate().test(i)){
        return state;
      }
    }
    throw new IllegalStateException("Cannot have no state");
  }

  public static <T extends  Comparable<T>, A extends  Comparable<A>> List<CompareState>
  getStateArray(Iterable<Function<T, A>> functionList, T t1, T t2){
    val outList = ImmutableList.<CompareState>builder();
    for (val function : functionList){
      A value1 = function.apply(t1);
      A value2 = function.apply(t2);
      val compareState = CompareState.getState(value1, value2);
      outList.add(compareState);
    }
    return outList.build();
  }

  public static <T extends  Comparable<T>, A extends  Comparable<A>> CompareState getState(Iterable<Function<T,A>> functions, T t1, T t2){
    val flist = getStateArray(functions, t1, t2);
    val firstDifferentOptional = flist.stream().filter(x -> x != EQ).findFirst();
    return firstDifferentOptional.orElse(EQ);
  }

}
