package org.collaboratory.ga4gh.core;

import static com.google.common.base.Preconditions.checkArgument;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;

import java.util.List;
import java.util.Map;

import org.elasticsearch.search.SearchHit;

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
  public static Map<String, Object> convertSourceToMap(final Map<String, Object> source, String field) {
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
    return convertSourceToObjectList(getSource(hit), field).stream()
        .map(o -> Integer.parseInt(o.toString()))
        .collect(toImmutableList());
  }

  public static List<String> convertHitToStringList(final SearchHit hit, String field) {
    return convertSourceToObjectList(getSource(hit), field).stream()
        .map(o -> o.toString())
        .collect(toImmutableList());
  }

  public static Map<String, Object> convertHitToObjectMap(final SearchHit hit, String field) {
    return convertSourceToMap(getSource(hit), field);
  }

}
