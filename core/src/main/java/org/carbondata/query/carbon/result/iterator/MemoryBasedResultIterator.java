package org.carbondata.query.carbon.result.iterator;

import org.carbondata.core.iterator.CarbonIterator;
import org.carbondata.query.carbon.result.Result;

/**
 * Memory based result iterator
 */
public class MemoryBasedResultIterator implements CarbonIterator<Result> {

	/**
	 * query result
	 */
	private Result result;

	/**
	 * to check any more
	 */
	private boolean hasNext = true;

	public MemoryBasedResultIterator(Result result) {
		this.result = result;
	}

	/**
	 * Returns {@code true} if the iteration has more elements. (In other words,
	 * returns {@code true} if {@link #next} would return an element rather than
	 * throwing an exception.)
	 *
	 * @return {@code true} if the iteration has more elements
	 */
	@Override
	public boolean hasNext() {
		return hasNext;
	}

	/**
	 * Returns the next element in the iteration.
	 *
	 * @return the next element in the iteration
	 */
	@Override
	public Result next() {
		hasNext = false;
		return result;
	}

}
