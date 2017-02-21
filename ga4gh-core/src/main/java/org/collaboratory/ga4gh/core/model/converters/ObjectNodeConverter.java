package org.collaboratory.ga4gh.core.model.converters;

import com.fasterxml.jackson.databind.node.ObjectNode;

public interface ObjectNodeConverter<T> {

  ObjectNode convertToObjectNode(T t);

}
