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

package org.icgc.dcc.ga4gh.loader.utils;

import lombok.NoArgsConstructor;
import org.icgc.dcc.common.core.util.stream.Streams;

import java.util.function.Function;
import java.util.stream.Stream;

import static lombok.AccessLevel.PRIVATE;

@NoArgsConstructor(access = PRIVATE)
public class Strings {

  /**
   * Converts input iterable to a String[] based on the defined mapping
   * @param objects iterable to be converted to String[]
   * @param mapping that converts each element of the iterable to a string
   * @param <T> generic type of elements in iterable
   * @return array of string representation of objects
   */
  public static <T> String[] toStringArray(final Iterable<T> objects, Function<T, ? extends String> mapping){
    return Streams.stream(objects)
        .map(mapping)
        .toArray(String[]::new);
  }

  /**
   * Converts input array to a String[] based on the defined mapping
   * @param array to be converted to String[]
   * @param mapping that converts each element of the array to a string
   * @param <T> generic type of elements in array
   * @return String[] of string representation of array
   */
  public static <T> String[] toStringArray(final T[] array, Function<T, ? extends String> mapping){
    return Stream.of(array)
        .map(mapping)
        .toArray(String[]::new);
  }

  /**
   * Converts input iterable to a String[], by calling Object::toString method on the object
   * @param objects iterable to be converted to String[]
   * @param <T> generic type of elements in iterable
   * @return array of string representation of objects
   */
  public static <T> String[] toStringArray(final Iterable<T> objects){
    return toStringArray(objects, Object::toString);
  }

  /**
   * Converts input array to a String[], by calling Object::toString method on the object
   * @param array to be converted to String[]
   * @param <T> generic type of elements in array
   * @return array of string representation of objects
   */
  public static <T> String[] toStringArray(final T[] array){
    return toStringArray(array, Object::toString);
  }

}
