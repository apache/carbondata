package org.apache.carbondata.store.api;

import java.io.IOException;

public interface Segment {

  void open() throws IOException;

  void commit() throws IOException;

  void abort() throws IOException;

  CarbonFileWriter newWriter() throws IOException;

}
