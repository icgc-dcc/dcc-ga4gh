package org.collaboratory.ga4gh.loader.utils;

public interface Countable<T> {

  void incr();

  void incr(T amount);

  void reset();

  T getCount();

}
