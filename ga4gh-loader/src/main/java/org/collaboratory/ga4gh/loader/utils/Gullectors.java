/*
 * Copyright (c) 2016 The Ontario Institute for Cancer Research. All rights reserved.
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

import static lombok.AccessLevel.PRIVATE;

import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collector;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import lombok.NoArgsConstructor;

@NoArgsConstructor(access = PRIVATE)
public final class Gullectors {

  public static <T> Collector<T, ImmutableSet.Builder<T>, Set<T>> immutableSetCollector() {
    return Collector.of(ImmutableSet.Builder<T>::new, ImmutableSet.Builder<T>::add, (s, r) -> s.addAll(r.build()),
        ImmutableSet.Builder<T>::build);
  }

  public static <T> Collector<T, ImmutableList.Builder<T>, List<T>> immutableListCollector() {
    return Collector.of(ImmutableList.Builder<T>::new, ImmutableList.Builder<T>::add, (s, r) -> s.addAll(r.build()),
        ImmutableList.Builder<T>::build);
  }

  public static <T, K, V> Collector<T, ImmutableMap.Builder<K, V>, ImmutableMap<K, V>> immutableMapCollector(
      Function<? super T, ? extends K> keyMapper,
      Function<? super T, ? extends V> valueMapper) {

    return Collector.of(ImmutableMap.Builder<K, V>::new,
        (r, t) -> r.put(keyMapper.apply(t), valueMapper.apply(t)),
        (l, r) -> l.putAll(r.build()),
        ImmutableMap.Builder<K, V>::build,
        Collector.Characteristics.UNORDERED);
  }

}
