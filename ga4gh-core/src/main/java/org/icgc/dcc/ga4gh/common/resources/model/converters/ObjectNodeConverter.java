package org.icgc.dcc.ga4gh.common.resources.model.converters;

import com.fasterxml.jackson.databind.node.ObjectNode;

public interface ObjectNodeConverter<T> {

  ObjectNode convertToObjectNode(T t);

}
