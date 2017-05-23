package org.icgc.dcc.ga4gh.loader.utils;

import lombok.NoArgsConstructor;
import org.icgc.dcc.common.core.util.stream.Streams;

import java.util.function.Function;
import java.util.stream.Stream;

import static lombok.AccessLevel.PRIVATE;

@NoArgsConstructor(access = PRIVATE)
public class Strings {

  /**
   * Converts input iterable to a String[] based on the defined mapping
   * @param objects iterable to be converted to String[]
   * @param mapping that converts each element of the iterable to a string
   * @param <T> generic type of elements in iterable
   * @return array of string representation of objects
   */
  public static <T> String[] toStringArray(final Iterable<T> objects, Function<T, ? extends String> mapping){
    return Streams.stream(objects)
        .map(mapping)
        .toArray(String[]::new);
  }

  /**
   * Converts input array to a String[] based on the defined mapping
   * @param array to be converted to String[]
   * @param mapping that converts each element of the array to a string
   * @param <T> generic type of elements in array
   * @return String[] of string representation of array
   */
  public static <T> String[] toStringArray(final T[] array, Function<T, ? extends String> mapping){
    return Stream.of(array)
        .map(mapping)
        .toArray(String[]::new);
  }

  /**
   * Converts input iterable to a String[], by calling Object::toString method on the object
   * @param objects iterable to be converted to String[]
   * @param <T> generic type of elements in iterable
   * @return array of string representation of objects
   */
  public static <T> String[] toStringArray(final Iterable<T> objects){
    return toStringArray(objects, Object::toString);
  }

  /**
   * Converts input array to a String[], by calling Object::toString method on the object
   * @param array to be converted to String[]
   * @param <T> generic type of elements in array
   * @return array of string representation of objects
   */
  public static <T> String[] toStringArray(final T[] array){
    return toStringArray(array, Object::toString);
  }

}
