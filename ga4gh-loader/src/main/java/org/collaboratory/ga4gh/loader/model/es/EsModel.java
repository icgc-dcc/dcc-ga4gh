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
package org.collaboratory.ga4gh.loader.model.es;

import static org.collaboratory.ga4gh.core.TypeChecker.isObjectCollection;
import static org.collaboratory.ga4gh.core.TypeChecker.isObjectMap;
import static org.icgc.dcc.common.core.json.JsonNodeBuilders.object;

import java.util.Collection;
import java.util.Map;

import org.icgc.dcc.common.core.json.JsonNodeBuilders;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import lombok.val;

//TODO: [rtisma]  move this to ga4gh-core, along with everything under model.es. 
public interface EsModel {

  ObjectNode toDocument();

  String getName(); // Every model must have a uniquely identifying name is a certain scope

  public static <T> ArrayNode createStringArrayNode(Iterable<T> values) {
    val array = JsonNodeBuilders.array();
    values.forEach(o -> array.with(o.toString()));
    return array.end();
  }

  public static ArrayNode createIntegerArrayNode(Iterable<Integer> values) {
    val array = JsonNodeBuilders.array();
    values.forEach(i -> array.with(i));
    return array.end();
  }

  public static ArrayNode createJsonNodeArrayNode(Iterable<JsonNode> values) {
    val builder = JsonNodeBuilders.array();
    values.forEach(v -> builder.with(v));
    return builder.end();
  }

  public static void main(String[] args) throws JsonProcessingException {
    val stringMap = Maps.<String, String> newHashMap();
    stringMap.put("mykey1", "myvalue1");
    stringMap.put("mykey2", "myvalue2");
    stringMap.put("mykey3", "myvalue3");
    val integerMap = Maps.<String, Integer> newHashMap();
    integerMap.put("mykey4", 4);
    integerMap.put("mykey5", 5);
    integerMap.put("mykey6", 6);

    val stringList = Lists.<String> newArrayList("myString1", "myString2", "myString3");
    val objectList = Lists.<Object> newArrayList("myObject1", 2, 3.333);
    val integerList = Lists.<Integer> newArrayList(11, 22, 33);

    val objectMap = Maps.<String, Object> newHashMap();
    objectMap.put("mykey7", "myobject7");
    objectMap.put("mykey8", 8);
    objectMap.put("mykey9", 9.99);
    objectMap.put("mykey10_integerMap", integerMap);
    objectMap.put("mykey11_stringMap", stringMap);
    objectMap.put("mykey12_stringList", stringList);
    objectMap.put("mykey13_objectList", objectList);
    objectMap.put("mykey14_integerList", integerList);

    objectMap.put("mirrorObjectMap", objectMap.clone());

    val objectMapNode = convertMapToObjectNode(objectMap);
    val mapper = new ObjectMapper();
    System.out.println("Tooo: " + mapper.writerWithDefaultPrettyPrinter().writeValueAsString(objectMapNode));

  }

  public static <K, V> ObjectNode convertMapToObjectNode(Map<K, V> map) {
    val objectNode = object();
    for (val entry : map.entrySet()) {
      val key = entry.getKey().toString();
      val value = entry.getValue();
      if (isObjectCollection(value)) {

        @SuppressWarnings("unchecked")
        val collection = (Collection<Object>) value;
        val arrayNode = createStringArrayNode(collection);
        objectNode.with(key, arrayNode);
      } else if (isObjectMap(value)) { // TODO: still incomplete

        @SuppressWarnings("unchecked")
        val innerMap = (Map<Object, Object>) value;
        objectNode.with(key, convertMapToObjectNode(innerMap));

      } else { // Treat everything else as just a string
        objectNode.with(key, value.toString());
      }
    }
    return objectNode.end();
  }

}