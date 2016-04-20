/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.carbondata.core.cache.dictionary;

import java.util.Iterator;
import java.util.List;

/**
 * The wrapper class wraps the list<list<bye[]>> and provide the iterator to retrieve the chunks
 * members and expose the getSize API to get size of members in the List<List<byte>> chunks.
 * Applications Scenario:
 * For preparing the column Sort info while writing the sort index file.
 */
public class DictionaryChunksWrapper implements Iterator<byte[]> {

    /**
     * list of dictionaryChunks
     */
    private List<List<byte[]>> dictionaryChunks;

    /**
     * size of the list
     */
    private int size;

    /**
     * Current index of the list
     */
    private int currentIndex = 0;

    /**
     * variable holds the count of elements already iterated
     */
    private int iteratedListSize = 0;

    /**
     * variable holds the current index of List<List<byte[]>> being traversed
     */
    private int outerIndex = 0;

    /**
     * Constructor of DictionaryChunksWrapper
     * @param dictionaryChunks
     */
    public DictionaryChunksWrapper(List<List<byte[]>> dictionaryChunks) {
        this.dictionaryChunks = dictionaryChunks;
        for (List<byte[]> chunk : dictionaryChunks) {
            this.size += chunk.size();
        }
    }

    /**
     * Returns {@code true} if the iteration has more elements.
     * (In other words, returns {@code true} if {@link #next} would
     * return an element rather than throwing an exception.)
     *
     * @return {@code true} if the iteration has more elements
     */
    @Override public boolean hasNext() {
        return (currentIndex < size);
    }

    /**
     * Returns the next element in the iteration.
     * The method pics the next elements from the first inner list till first is not finished, pics
     * the second inner list ...
     *
     * @return the next element in the iteration
     */
    @Override public byte[] next() {
        int index = currentIndex++;
        index = index - iteratedListSize;
        while (index > dictionaryChunks.get(outerIndex).size()) {
            int innerListSize = dictionaryChunks.get(outerIndex).size();
            iteratedListSize += innerListSize;
            index = index - innerListSize;
            outerIndex++;
        }
        return dictionaryChunks.get(outerIndex).get(index);
    }

    /**
     * Removes from the underlying collection the last element returned
     * by this iterator (optional operation).  This method can be called
     * only once per call to {@link #next}.  The behavior of an iterator
     * is unspecified if the underlying collection is modified while the
     * iteration is in progress in any way other than by calling this
     * method.
     *
     * @throws UnsupportedOperationException if the {@code remove}
     *                                       operation is not supported by this iterator
     * @throws IllegalStateException         if the {@code next} method has not
     *                                       yet been called, or the {@code remove} method has already
     *                                       been called after the last call to the {@code next}
     *                                       method
     * @implSpec The default implementation throws an instance of
     * {@link UnsupportedOperationException} and performs no other action.
     */
    @Override public void remove() {
        throw new UnsupportedOperationException("Remove operation not supported");
    }

    /**
     * returns the total element size in List<List<byte[]>>
     *
     * @return
     */
    public int getSize() {
        return size;
    }
}
