package org.apache.carbondata.core.partition;

/**
 * Partitions the data as per key
 */
public interface Partitioner<Key> {

  int getPartition(Key key);

}
