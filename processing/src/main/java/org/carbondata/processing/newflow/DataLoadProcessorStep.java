package org.carbondata.processing.newflow;

import java.util.Iterator;
import java.util.List;

public interface DataLoadProcessorStep {

  DataField[] getOutput();

  List<Iterator<Object[]>> execute();

}
