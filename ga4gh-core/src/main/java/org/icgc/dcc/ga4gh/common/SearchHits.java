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

import org.elasticsearch.search.SearchHit;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableSet;

public class SearchHits {

  public static Map<String, Object> getSource(final SearchHit hit) {
    checkArgument(hit.hasSource(), "Hit doesn not have source");
    return hit.getSource();
  }
  
  
  private static Object getAttributeFromSource(final Map<String, Object> source, String attr){
    checkArgument(source.containsKey(attr), "Source does not have the attribute: {}", attr);
    return source.get(attr);
  }

  public static String convertSourceToString(final Map<String, Object> source, String attr) {
    return getAttributeFromSource(source, attr).toString();
  }

  public static Boolean convertSourceToBoolean(final Map<String, Object> source, String attr) {
    return Boolean.parseBoolean(convertSourceToString(source, attr));
  }

  public static Integer convertSourceToInteger(final Map<String, Object> source, String attr) {
    return Integer.parseInt(convertSourceToString(source, attr));
  }

  public static Long convertSourceToLong(final Map<String, Object> source, String attr) {
    return Long.parseLong(convertSourceToString(source, attr));
  }

  public static Double convertSourceToDouble(final Map<String, Object> source, String attr) {
    return Double.parseDouble(convertSourceToString(source, attr));
  }

  // TODO: [rtisma] verify this is the correct way to extract an array result
  @SuppressWarnings("unchecked")
  public static List<Object> convertSourceToObjectList(final Map<String, Object> source, String field) {
    return (List<Object>) getAttributeFromSource(source, field);
  }

  @SuppressWarnings("unchecked")
  public static List<Integer> convertSourceToIntegerList(final Map<String, Object> source, String field) {
    return convertSourceToObjectList(source, field).stream()
        .map(o -> Integer.parseInt(o.toString()))
        .collect(toImmutableList());
  }

  public static Set<Integer> convertSourceToIntegerSet(final Map<String, Object> source, String field) {
    return convertSourceToObjectList(source, field).stream()
        .map(o -> Integer.parseInt(o.toString()))
        .collect(toImmutableSet());
  }

  @SuppressWarnings("unchecked")
  public static List<String> convertSourceToStringList(final Map<String, Object> source, String field) {
    return convertSourceToObjectList(source, field).stream()
        .map(Object::toString)
        .collect(toImmutableList());
  }

  @SuppressWarnings("unchecked")
  public static Map<String, Object> convertSourceToObjectMap(final Map<String, Object> source, String field) {
    return (Map<String, Object>) getAttributeFromSource(source, field);
  }

  public static String convertHitToString(final SearchHit hit, String attr) {
    return convertSourceToString(getSource(hit), attr);
  }

  public static Boolean convertHitToBoolean(final SearchHit hit, String attr) {
    return convertSourceToBoolean(getSource(hit), attr);
  }

  public static Integer convertHitToInteger(final SearchHit hit, String attr) {
    return convertSourceToInteger(getSource(hit), attr);
  }

  public static Long convertHitToLong(final SearchHit hit, String attr) {
    return convertSourceToLong(getSource(hit), attr);
  }

  public static Double convertHitToDouble(final SearchHit hit, String attr) {
    return convertSourceToDouble(getSource(hit), attr);
  }

  public static List<Object> convertHitToObjectList(final SearchHit hit, String field) {
    return convertSourceToObjectList(getSource(hit), field);
  }

  public static List<Integer> convertHitToIntegerList(final SearchHit hit, String field) {
    return convertSourceToIntegerList(getSource(hit), field);
  }

  public static List<String> convertHitToStringList(final SearchHit hit, String field) {
    return convertSourceToStringList(getSource(hit), field);
  }

  public static Map<String, Object> convertHitToObjectMap(final SearchHit hit, String field) {
    return convertSourceToObjectMap(getSource(hit), field);
  }

}
