package org.apache.carbondata.processing.constants;

/**
 * enum to hold the bad record logger action
 */
public enum LoggerAction {

  FORCE("FORCE"), // data will be converted to null
  REDIRECT("REDIRECT"), // no null conversion moved to bad record and written to raw csv
  IGNORE("IGNORE"); // no null conversion moved to bad record and not written to raw csv

  private String name;

  LoggerAction(String name) {
    this.name = name;
  }

  @Override public String toString() {
    return this.name;
  }
}
