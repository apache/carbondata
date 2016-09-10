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

package org.apache.carbondata.processing.csvreaderstep;

import java.util.List;

import org.apache.carbondata.core.load.BlockDetails;

/**
 * Vo class which will holds all the properties required to read and parse the
 * csv file
 */
public class UnivocityCsvParserVo {

  /**
   * delimiter of the records
   */
  private String delimiter;

  /**
   * file encoding
   */
  private String encoding;

  /**
   * is header present in the file
   */
  private boolean headerPresent;

  /**
   * enclosure
   */
  private String enclosure;

  /**
   * escape enclosure
   */
  private boolean escapeEnclosure;

  /**
   * number of columns
   */
  private int numberOfColumns;

  /**
   * block details list, which will have
   * all the detail if the block
   */
  private List<BlockDetails> blockDetailsList;

  /**
   * escape character;
   */
  private String escapeCharacter;

  /**
   * quote character;
   */
  private String quoteCharacter;

  /**
   * comment character;
   */
  private String commentCharacter;

  /**
   * max number of columns configured by user to be parsed in a row
   */
  private int maxColumns;

  /**
   * @return the delimiter
   */
  public String getDelimiter() {
    return delimiter;
  }

  /**
   * @param delimiter the delimiter to set
   */
  public void setDelimiter(String delimiter) {
    this.delimiter = delimiter;
  }

  /**
   * @return the encoding
   */
  public String getEncoding() {
    return encoding;
  }

  /**
   * @param encoding the encoding to set
   */
  public void setEncoding(String encoding) {
    this.encoding = encoding;
  }

  /**
   * @return the headerPresent
   */
  public boolean isHeaderPresent() {
    return headerPresent;
  }

  /**
   * @param headerPresent the headerPresent to set
   */
  public void setHeaderPresent(boolean headerPresent) {
    this.headerPresent = headerPresent;
  }

  /**
   * @return the enclosure
   */
  public String getEnclosure() {
    return enclosure;
  }

  /**
   * @param enclosure the enclosure to set
   */
  public void setEnclosure(String enclosure) {
    this.enclosure = enclosure;
  }

  /**
   * @return the escapeEnclosure
   */
  public boolean isEscapeEnclosure() {
    return escapeEnclosure;
  }

  /**
   * @param escapeEnclosure the escapeEnclosure to set
   */
  public void setEscapeEnclosure(boolean escapeEnclosure) {
    this.escapeEnclosure = escapeEnclosure;
  }

  /**
   * @return the numberOfColumns
   */
  public int getNumberOfColumns() {
    return numberOfColumns;
  }

  /**
   * @param numberOfColumns the numberOfColumns to set
   */
  public void setNumberOfColumns(int numberOfColumns) {
    this.numberOfColumns = numberOfColumns;
  }

  /**
   * @return the blockDetailsList
   */
  public List<BlockDetails> getBlockDetailsList() {
    return blockDetailsList;
  }

  /**
   * @param blockDetailsList the blockDetailsList to set
   */
  public void setBlockDetailsList(List<BlockDetails> blockDetailsList) {
    this.blockDetailsList = blockDetailsList;
  }

  /**
   * @return the escapeCharacter
   */
  public String getEscapeCharacter() {
    return escapeCharacter;
  }

  /**
   * @param escapeCharacter the escapeCharacter to set
   */
  public void setEscapeCharacter(String escapeCharacter) {
    this.escapeCharacter = escapeCharacter;
  }

  public String getQuoteCharacter() { return quoteCharacter; }

  public void setQuoteCharacter(String quoteCharacter) { this.quoteCharacter = quoteCharacter; }

  public String getCommentCharacter() { return commentCharacter; }

  public void setCommentCharacter(String commentCharacter) {
    this.commentCharacter = commentCharacter;
  }


  /**
   * @return
   */
  public int getMaxColumns() {
    return maxColumns;
  }

  /**
   * @param maxColumns
   */
  public void setMaxColumns(int maxColumns) {
    this.maxColumns = maxColumns;
  }
}
