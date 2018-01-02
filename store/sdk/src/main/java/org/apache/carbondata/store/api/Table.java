package org.apache.carbondata.store.api;

public interface Table {
  Segment newBatchSegment();
}
