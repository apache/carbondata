package org.carbondata.query.carbon.result.iterator;

import org.carbondata.core.iterator.CarbonIterator;
import org.carbondata.query.carbon.result.BatchRawResult;

public class ChunkRawRowIterartor extends CarbonIterator<Object[]> {

  /**
   * iterator over chunk result
   */
  private CarbonIterator<BatchRawResult> iterator;

  /**
   * currect chunk
   */
  private BatchRawResult currentchunk;

  public ChunkRawRowIterartor(CarbonIterator<BatchRawResult> iterator) {
    this.iterator = iterator;
    if (iterator.hasNext()) {
      currentchunk = iterator.next();
    }
  }

  /**
   * Returns {@code true} if the iteration has more elements. (In other words,
   * returns {@code true} if {@link #next} would return an element rather than
   * throwing an exception.)
   *
   * @return {@code true} if the iteration has more elements
   */
  @Override public boolean hasNext() {
    if (null != currentchunk) {
      if ((currentchunk.hasNext())) {
        return true;
      } else if (!currentchunk.hasNext()) {
        while (iterator.hasNext()) {
          currentchunk = iterator.next();
          if (currentchunk != null && currentchunk.hasNext()) {
            return true;
          }
        }
      }
    }
    return false;
  }

  /**
   * Returns the next element in the iteration.
   *
   * @return the next element in the iteration
   */
  @Override public Object[] next() {
    return currentchunk.next();
  }
}
