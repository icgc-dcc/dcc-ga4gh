package org.collaboratory.ga4gh.server.util;

import java.util.List;
import java.util.Map;

import org.elasticsearch.search.SearchHit;

public class SearchHitConverters {

  public static String convertHitToString(final SearchHit hit, String attr) {
    return hit.getSource().get(attr).toString();
  }

  public static Long convertHitToLong(final SearchHit hit, String attr) {
    return Long.parseLong(hit.getSource().get(attr).toString());
  }

  // TODO: [rtisma] verify this is the correct way to extract an array result
  @SuppressWarnings("unchecked")
  public
  static List<Object> convertHitToArray(final SearchHit hit, String field) {
    return (List<Object>) hit.getSource().get(field);
  }

  @SuppressWarnings("unchecked")
  public
  static Map<String, Object> convertHitToMap(final SearchHit hit, String field) {
    return (Map<String, Object>) hit.getSource().get(field);
  }

}
