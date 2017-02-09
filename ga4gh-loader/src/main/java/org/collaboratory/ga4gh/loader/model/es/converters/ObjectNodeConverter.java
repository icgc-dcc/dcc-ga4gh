package org.collaboratory.ga4gh.loader.model.es.converters;

import com.fasterxml.jackson.databind.node.ObjectNode;

public interface ObjectNodeConverter<T> {

  ObjectNode convertToObjectNode(T t);

}
