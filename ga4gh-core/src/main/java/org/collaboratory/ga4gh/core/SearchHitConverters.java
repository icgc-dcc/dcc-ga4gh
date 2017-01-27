package org.collaboratory.ga4gh.core;

import java.util.List;
import java.util.Map;

import org.elasticsearch.search.SearchHit;
import org.icgc.dcc.common.core.util.stream.Collectors;

public class SearchHitConverters {

  public static Map<String, Object> getSource(final SearchHit hit) {
    return hit.getSource();
  }

  public static String convertSourceToString(final Map<String, Object> source, String attr) {
    return source.get(attr).toString();
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
    return (List<Object>) source.get(field);
  }

  @SuppressWarnings("unchecked")
  public static Map<String, Object> convertSourceToMap(final Map<String, Object> source, String field) {
    return (Map<String, Object>) source.get(field);
  }

  public static String convertHitToString(final SearchHit hit, String attr) {
    return convertSourceToString(getSource(hit), attr);
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
        .collect(Collectors.toImmutableList());
  }

  public static List<String> convertHitToStringList(final SearchHit hit, String field) {
    return convertSourceToObjectList(getSource(hit), field).stream()
        .map(o -> o.toString())
        .collect(Collectors.toImmutableList());
  }

  public static Map<String, Object> convertHitToObjectMap(final SearchHit hit, String field) {
    return convertSourceToMap(getSource(hit), field);
  }

}
