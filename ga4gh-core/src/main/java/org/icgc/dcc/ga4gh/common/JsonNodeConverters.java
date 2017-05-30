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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.NoArgsConstructor;
import lombok.val;

import java.util.Collection;
import java.util.Map;

import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.common.core.json.JsonNodeBuilders.array;
import static org.icgc.dcc.common.core.json.JsonNodeBuilders.object;

@NoArgsConstructor(access = PRIVATE)
public class JsonNodeConverters {

  public static <K, V> ObjectNode convertMap(Map<K, V> map) {
    val objectNode = object();
    for (val entry : map.entrySet()) {
      val key = entry.getKey().toString();
      val value = entry.getValue();
      if (TypeChecker.isObjectCollection(value)) {

        @SuppressWarnings("unchecked")
        val collection = (Collection<Object>) value;
        val arrayNode = convertStrings(collection);
        objectNode.with(key, arrayNode);
      } else if (TypeChecker.isObjectMap(value)) {

        @SuppressWarnings("unchecked")
        val innerMap = (Map<Object, Object>) value;
        objectNode.with(key, convertMap(innerMap));

      } else { // Treat everything else as just a string
        objectNode.with(key, value.toString());
      }
    }
    return objectNode.end();
  }

  public static ArrayNode convertIntegers(Iterable<Integer> values) {
    val array = array();
    values.forEach(array::with);
    return array.end();
  }

  public static <T extends JsonNode> ArrayNode convertJsonNodes(Iterable<T> values) {
    val builder = array();
    values.forEach(builder::with);
    return builder.end();
  }

  public static <T> ArrayNode convertStrings(Iterable<T> values) {
    val array = array();
    values.forEach(o -> array.with(o.toString()));
    return array.end();
  }

}
