package org.icgc.dcc.ga4gh.server.performance;

import ga4gh.VariantServiceOuterClass.SearchVariantsRequest;
import lombok.NonNull;
import lombok.val;
import org.eclipse.collections.impl.factory.Stacks;

import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;

public class SearchRequestSweepIterator implements Iterator<SearchVariantsRequest.Builder> {

  public static SearchRequestSweepIterator createSearchRequestSweepIterator(
      List<SubSearchVariantRequestIterator> list) {
    return new SearchRequestSweepIterator(list);
  }

  @NonNull private final List<SubSearchVariantRequestIterator> list;

  private SubSearchVariantRequestIterator currentSubIterator;
  private Iterator<SubSearchVariantRequestIterator> listIterator;

  private SearchRequestSweepIterator(
      List<SubSearchVariantRequestIterator> list) {
    this.list = list;
    checkState(!list.isEmpty(), "the list is empty");
    listIterator = list.iterator();
    currentSubIterator = listIterator.next();
  }

  public boolean hasNext2() {
    if (currentSubIterator.hasNext()){
      return true;
    } else if(listIterator.hasNext()) {
      currentSubIterator = listIterator.next();
      return hasNext();
    }
    return false;
  }

  public boolean hasNext(){
    val stack = Stacks.mutable.of(currentSubIterator);
    boolean result = false;
    while(stack.size() > 0){
      val cc = stack.pop();
      if (cc.hasNext()){
        result = true;
      } else if (listIterator.hasNext()){
        currentSubIterator = listIterator.next();
        stack.push(currentSubIterator);
      }
    }
    return result;
  }

  public int getSize(){
    int result = 0;
    for (val sub : list){
      result+= sub.getSize();
    }
    return result;
  }

  @Override public SearchVariantsRequest.Builder next() {
    return currentSubIterator.next();
  }
}
