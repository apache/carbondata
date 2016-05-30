package org.carbondata.hadoop;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * User can add required columns
 */
public class CarbonProjection {

  private Set<String> columns = new LinkedHashSet<>();

  public void addColumn(String column) {
    columns.add(column);
  }

  public String[] getAllColumns() {
    return columns.toArray(new String[columns.size()]);
  }

  public boolean isEmpty() {
    return columns.isEmpty();
  }
}
