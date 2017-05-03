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
