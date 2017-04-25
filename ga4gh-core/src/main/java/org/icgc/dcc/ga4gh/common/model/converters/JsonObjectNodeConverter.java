package org.icgc.dcc.ga4gh.common.model.converters;

import com.fasterxml.jackson.databind.node.ObjectNode;

public interface JsonObjectNodeConverter<T> {

  ObjectNode convertToObjectNode(T t);

}
