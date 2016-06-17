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

package org.carbondata.processing.csvreaderstep;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.load.BlockDetails;
import org.carbondata.processing.surrogatekeysgenerator.csvbased.BadRecordslogger;
import org.carbondata.processing.util.CarbonDataProcessorUtil;

import org.apache.commons.vfs.FileObject;
import org.pentaho.di.core.exception.KettleConversionException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.textfileinput.EncodingType;

/**
 * this class use to handle multiple blocks in multiple threads
 * each thread hold one blockDataHandler to process block data
 */
public class BlockDataHandler {
  public byte[] byteBuffer;
  public int startBuffer;
  public int endBuffer;
  public int bufferSize;
  public long bytesToSkipInFirstFile;
  public long totalBytesRead;
  public CsvInputMeta meta;
  public CsvInputData data;
  public TransMeta transMeta;
  public boolean isNeedToSkipFirstLineInBlock;
  public long currentOffset;

  protected InputStream bufferedInputStream;
  protected BadRecordslogger badRecordslogger;
  private String badRecordFileName;

  public BlockDataHandler() {
    byteBuffer = new byte[] {};
    isNeedToSkipFirstLineInBlock = true;
    currentOffset = 0;

  }

  // Resize
  public void resizeByteBufferArray() {
    // What's the new size?
    // It's (endBuffer-startBuffer)+size !!
    // That way we can at least read one full block of data using NIO

    bufferSize = endBuffer - startBuffer;
    int newSize = bufferSize + data.preferredBufferSize;
    byte[] newByteBuffer = new byte[newSize + 100];

    // copy over the old data...
    System.arraycopy(byteBuffer, startBuffer, newByteBuffer, 0, bufferSize);

    // replace the old byte buffer...
    byteBuffer = newByteBuffer;

    // Adjust start and end point of data in the byte buffer
    //
    startBuffer = 0;
    endBuffer = bufferSize;
  }

  public int readBufferFromFile() throws IOException {
    // See if the line is not longer than the buffer.
    // In that case we need to increase the size of the byte buffer.
    // Since this method doesn't get called every other character,
    // I'm sure we can spend a bit of time here without major performance loss.
    //

    int read = bufferedInputStream.read(byteBuffer, endBuffer, (byteBuffer.length - endBuffer));
    if (read >= 0) {
      // adjust the highest used position...
      //
      bufferSize = endBuffer + read;
    }
    return read;
  }

  /**
   * Increase the endBuffer pointer by one.<br>
   * If there is not enough room in the buffer to go there, resize the byte buffer and read more
   * data.<br>
   * if there is no more data to read and if the endBuffer pointer has reached the end of the byte
   * buffer, we return true.<br>
   *
   * @return true if we reached the end of the byte buffer.
   * @throws IOException In case we get an error reading from the input file.
   */
  public boolean increaseEndBuffer() throws IOException {
    endBuffer++;
    currentOffset++;
    if (endBuffer >= bufferSize) {
      // Oops, we need to read more data...
      // Better resize this before we read other things in it...
      //
      resizeByteBufferArray();

      // Also read another chunk of data, now that we have the space for it...
      //
      int n = readBufferFromFile();

      // Return true we didn't manage to read anything and we reached the end of the buffer...
      //
      return n < 0;
    }

    return false;
  }

  /**
   * <pre>
   * [abcd "" defg] --> [abcd " defg]
   * [""""] --> [""]
   * [""] --> ["]
   * </pre>
   *
   * @return the byte array with escaped enclosures escaped.
   */
  public byte[] removeEscapedEnclosures(byte[] field, int nrEnclosuresFound) {
    byte[] result = new byte[field.length - nrEnclosuresFound];
    int resultIndex = 0;
    for (int i = 0; i < field.length; i++) {
      if (field[i] == data.enclosure[0]) {
        if (!(i + 1 < field.length && field[i + 1] == data.enclosure[0])) {
          // Not an escaped enclosure...
          result[resultIndex++] = field[i];
        }
      } else {
        result[resultIndex++] = field[i];
      }
    }
    return result;
  }

  protected boolean openFile(StepMetaInterface smi, StepDataInterface sdi, TransMeta trans,
      BlockDetails blockDetails) throws KettleException {
    try {
      this.meta = (CsvInputMeta) smi;
      this.data = (CsvInputData) sdi;
      this.transMeta = trans;
      // Close the previous file...
      if (this.bufferedInputStream != null) {
        this.bufferedInputStream.close();
      }

      // Open the next one...
      if (FileFactory.getFileType(blockDetails.getFilePath()) == FileFactory.FileType.HDFS
            || FileFactory.getFileType(blockDetails.getFilePath()) == FileFactory.FileType.VIEWFS) {
        //when case HDFS file type, we use the file path directly
        //give 0 offset as the file start offset when open a new file
        initializeFileReader(blockDetails.getFilePath(), 0);
      } else {
        FileObject fileObject = KettleVFS.getFileObject(blockDetails.getFilePath(), transMeta);
        initializeFileReader(fileObject);
      }

      this.isNeedToSkipFirstLineInBlock = true;

      // See if we need to skip a row...
      // - If you have a header row checked and if you're not running in parallel
      // - If you're running in parallel, if a header row is checked, if you're at
      // the beginning of a file
      if (meta.isHeaderPresent()) {
        if ((!data.parallel) || // Standard flat file : skip header
            (data.parallel && this.bytesToSkipInFirstFile <= 0)) {
          readOneRow(false);
        }
      }

      // Don't skip again in the next file...
      this.bytesToSkipInFirstFile = -1L;
      String key = meta.getDatabaseName() + '/' + meta.getTableName() + '_' + meta.getTableName();
      badRecordFileName = transMeta.getVariable("csvInputFilePath");
      badRecordFileName = null != badRecordFileName ? badRecordFileName : meta.getTableName();
      badRecordFileName = CarbonDataProcessorUtil.getBagLogFileName(badRecordFileName);
      badRecordslogger = new BadRecordslogger(key, badRecordFileName, CarbonDataProcessorUtil
          .getBadLogStoreLocation(meta.getDatabaseName() + '/' + meta.getTableName()));
      return true;
    } catch (KettleException e) {
      throw e;
    } catch (Exception e) {
      throw new KettleException(e);
    }
  }

  protected void initializeFileReader(FileObject fileObject) throws IOException {
    //using file object to get path can return a valid path which for new inputstream
    String filePath = KettleVFS.getFilename(fileObject);
    this.bufferedInputStream = FileFactory
        .getDataInputStream(filePath, FileFactory.getFileType(filePath), data.preferredBufferSize);
    //when open a new file, need to initialize all info
    this.byteBuffer = new byte[data.preferredBufferSize];
    this.bufferSize = 0;
    this.startBuffer = 0;
    this.endBuffer = 0;
    this.currentOffset = 0;
  }

  /**
   * skip the offset and reset the value
   *
   * @param filePath
   * @param offset
   * @throws IOException
   */
  protected void initializeFileReader(String filePath, long offset)
      throws IOException, KettleFileException {
    if (this.bufferedInputStream != null) {
      this.bufferedInputStream.close();
    }

    // handle local file path, return path which can be handle by inputstream
    if (FileFactory.getFileType(filePath) == FileFactory.FileType.LOCAL) {
      FileObject fileObject = KettleVFS.getFileObject(filePath, transMeta);
      filePath = KettleVFS.getFilename(fileObject);
    }

    this.bufferedInputStream = FileFactory
        .getDataInputStream(filePath, FileFactory.getFileType(filePath), data.preferredBufferSize,
            offset);
    this.byteBuffer = new byte[data.preferredBufferSize];
    this.bufferSize = 0;
    this.startBuffer = 0;
    this.endBuffer = 0;
    this.currentOffset = 0;
  }

  /**
   * Read a single row of data from the file...
   *
   * @param doConversions if you want to do conversions, set to false for the header row.
   * @return a row of data...
   * @throws KettleException
   */
  public Object[] readOneRow(boolean doConversions) throws KettleException {

    try {
      while (true) {
        Object[] outputRowData =
            RowDataUtil.allocateRowData(data.outputRowMeta.size() - RowDataUtil.OVER_ALLOCATE_SIZE);
        int outputIndex = 0;
        boolean newLineFound = false;
        boolean endOfBuffer = false;
        int newLines = 0;
        List<Exception> conversionExceptions = null;
        List<ValueMetaInterface> exceptionFields = null;

        // The strategy is as follows...
        // We read a block of byte[] from the file.
        // We scan for the separators in the file (NOT for line feeds etc)
        // Then we scan that block of data.
        // We keep a byte[] that we extend if needed..
        // At the end of the block we read another, etc.
        //
        // Let's start by looking where we left off reading.
        //
        while (!newLineFound && outputIndex < meta.getInputFields().length) {

          if (checkBufferSize() && outputRowData != null) {
            // Last row was being discarded if the last item is null and
            // there is no end of line delimiter
            //if (outputRowData != null) {
            // Make certain that at least one record exists before
            // filling the rest of them with null
            if (outputIndex > 0) {
              return (outputRowData);
            }

            return null; // nothing more to read, call it a day.
          }

          // OK, at this point we should have data in the byteBuffer and we should be able
          // to scan for the next
          // delimiter (;)
          // So let's look for a delimiter.
          // Also skip over the enclosures ("), it is NOT taking into account
          // escaped enclosures.
          // Later we can add an option for having escaped or double enclosures
          // in the file. <sigh>
          //
          boolean delimiterFound = false;
          boolean enclosureFound = false;
          int escapedEnclosureFound = 0;
          while (!delimiterFound) {
            // If we find the first char, we might find others as well ;-)
            // Single byte delimiters only for now.
            //
            if (data.delimiterMatcher
                .matchesPattern(this.byteBuffer, this.endBuffer, data.delimiter)) {
              delimiterFound = true;
            }
            // Perhaps we found a (pre-mature) new line?
            //
            else if (
                // In case we are not using an enclosure and in case fields contain new
                // lines we need to make sure that we check the newlines possible flag.
                // If the flag is enable we skip newline checking except for the last field
                // in the row. In that one we can't support newlines without
                // enclosure (handled below).
                (!meta.isNewlinePossibleInFields()
                    || outputIndex == meta.getInputFields().length - 1) && (
                    data.crLfMatcher.isReturn(this.byteBuffer, this.endBuffer) || data.crLfMatcher
                        .isLineFeed(this.byteBuffer, this.endBuffer))) {

              if (data.encodingType.equals(EncodingType.DOUBLE_LITTLE_ENDIAN) || data.encodingType
                  .equals(EncodingType.DOUBLE_BIG_ENDIAN)) {
                this.endBuffer += 2;
                this.currentOffset += 2;
              } else {
                this.endBuffer++;
                this.currentOffset++;
              }

              this.totalBytesRead++;
              newLines = 1;

              if (this.endBuffer >= this.bufferSize) {
                // Oops, we need to read more data...
                // Better resize this before we read other things in it...
                //
                this.resizeByteBufferArray();

                // Also read another chunk of data, now that we have the space for it...
                // Ignore EOF, there might be other stuff in the buffer.
                //
                this.readBufferFromFile();
              }

              // re-check for double delimiters...
              if (data.crLfMatcher.isReturn(this.byteBuffer, this.endBuffer) || data.crLfMatcher
                  .isLineFeed(this.byteBuffer, this.endBuffer)) {
                this.endBuffer++;
                this.currentOffset++;
                this.totalBytesRead++;
                newLines = 2;
                if (this.endBuffer >= this.bufferSize) {
                  // Oops, we need to read more data...
                  // Better resize this before we read other things in it...
                  //
                  this.resizeByteBufferArray();

                  // Also read another chunk of data, now that we have the space for
                  // it...
                  // Ignore EOF, there might be other stuff in the buffer.
                  //
                  this.readBufferFromFile();
                }
              }

              newLineFound = true;
              delimiterFound = true;
            }
            // Perhaps we need to skip over an enclosed part?
            // We always expect exactly one enclosure character
            // If we find the enclosure doubled, we consider it escaped.
            // --> "" is converted to " later on.
            //
            else if (data.enclosure != null && data.enclosureMatcher
                .matchesPattern(this.byteBuffer, this.endBuffer, data.enclosure)) {

              enclosureFound = true;
              boolean keepGoing;
              do {
                if (this.increaseEndBuffer()) {
                  enclosureFound = false;
                  break;
                }

                if (!doConversions) {
                  //when catch the block which need to skip first line
                  //the complete row like: abc,"cdf","efg",hij
                  //but this row is split to different blocks
                  //in this block,the remaining row like :  fg",hij
                  //so if we meet the enclosure in the skip line, when we meet \r or \n ,let's break
                  if (data.crLfMatcher.isReturn(this.byteBuffer, this.endBuffer) || data.crLfMatcher
                      .isLineFeed(this.byteBuffer, this.endBuffer)) {
                    enclosureFound = false;
                    break;
                  }
                }

                keepGoing = !data.enclosureMatcher
                    .matchesPattern(this.byteBuffer, this.endBuffer, data.enclosure);
                if (!keepGoing) {
                  // We found an enclosure character.
                  // Read another byte...
                  if (this.increaseEndBuffer()) {
                    enclosureFound = false;
                    break;
                  }

                  // If this character is also an enclosure, we can consider the
                  // enclosure "escaped".
                  // As such, if this is an enclosure, we keep going...
                  //
                  keepGoing = data.enclosureMatcher
                      .matchesPattern(this.byteBuffer, this.endBuffer, data.enclosure);
                  if (keepGoing) {
                    escapedEnclosureFound++;
                  } else {
                    /**
                     * <pre>
                     * fix for customer issue.
                     * after last enclosure there must be either field end or row
                     * end otherwise enclosure is field content.
                     * Example:
                     * EMPNAME, COMPANY
                     * 'emp'aa','comab'
                     * 'empbb','com'cd'
                     * Here enclosure after emp(emp') and after com(com')
                     * are not the last enclosures
                     * </pre>
                     */
                    keepGoing = !(data.delimiterMatcher
                        .matchesPattern(this.byteBuffer, this.endBuffer, data.delimiter)
                        || data.crLfMatcher.isReturn(this.byteBuffer, this.endBuffer)
                        || data.crLfMatcher.isLineFeed(this.byteBuffer, this.endBuffer));
                  }

                }
              } while (keepGoing);

              // Did we reach the end of the buffer?
              //
              if (this.endBuffer >= this.bufferSize) {
                newLineFound = true; // consider it a newline to break out of the upper
                // while loop
                newLines += 2; // to remove the enclosures in case of missing
                // newline on last line.
                endOfBuffer = true;
                break;
              }
            } else {

              this.endBuffer++;
              this.currentOffset++;
              this.totalBytesRead++;

              if (checkBufferSize()) {
                if (this.endBuffer >= this.bufferSize) {
                  newLineFound = true;
                  break;
                }
              }
            }
          }

          // If we're still here, we found a delimiter..
          // Since the starting point never changed really, we just can grab range:
          //
          //    [startBuffer-endBuffer[
          //
          // This is the part we want.
          // data.byteBuffer[data.startBuffer]
          //
          int length = calculateFieldLength(newLineFound, newLines, enclosureFound, endOfBuffer);

          byte[] field = new byte[length];
          System.arraycopy(this.byteBuffer, this.startBuffer, field, 0, length);

          // Did we have any escaped characters in there?
          //
          if (escapedEnclosureFound > 0) {
            field = this.removeEscapedEnclosures(field, escapedEnclosureFound);
          }

          if (doConversions) {
            if (meta.isLazyConversionActive()) {
              outputRowData[outputIndex++] = field;
            } else {
              // We're not lazy so we convert the data right here and now.
              // The convert object uses binary storage as such we just have to ask
              // the native type from it.
              // That will do the actual conversion.
              //
              ValueMetaInterface sourceValueMeta = data.convertRowMeta.getValueMeta(outputIndex);
              try {
                // when found a blank line, outputRowData will be filled as
                // Object array = ["@NU#LL$!BLANKLINE", null, null, ... ]
                if (field.length == 0 && newLineFound && outputIndex == 0) {
                  outputRowData[outputIndex++] = CarbonCommonConstants.BLANK_LINE_FLAG;
                } else {
                  outputRowData[outputIndex++] =
                      sourceValueMeta.convertBinaryStringToNativeType(field);
                }
              } catch (KettleValueException e) {
                // There was a conversion error,
                //
                outputRowData[outputIndex++] = null;

                if (conversionExceptions == null) {
                  conversionExceptions =
                      new ArrayList<Exception>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
                  exceptionFields =
                      new ArrayList<ValueMetaInterface>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
                }

                conversionExceptions.add(e);
                exceptionFields.add(sourceValueMeta);
              }
            }
          } else {
            outputRowData[outputIndex++] = null; // nothing for the header, no conversions here.
          }

          // OK, move on to the next field...
          if (!newLineFound) {
            this.endBuffer++;
            this.currentOffset++;
            this.totalBytesRead++;
          }
          this.startBuffer = this.endBuffer;
        }

        // See if we reached the end of the line.
        // If not, we need to skip the remaining items on the line until the next newline...
        if (!newLineFound && !checkBufferSize()) {
          while (!data.crLfMatcher.isReturn(this.byteBuffer, this.endBuffer) && !data.crLfMatcher
              .isLineFeed(this.byteBuffer, this.endBuffer)) {
            this.endBuffer++;
            this.currentOffset++;
            this.totalBytesRead++;

            if (checkBufferSize()) {
              break; // nothing more to read.
            }

            // HANDLE: if we're using quoting we might be dealing with a very dirty file
            // with quoted newlines in trailing fields. (imagine that)
            // In that particular case we want to use the same logic we use above
            // (refactored a bit) to skip these fields.

          }

          if (!checkBufferSize()) {
            while (data.crLfMatcher.isReturn(this.byteBuffer, this.endBuffer) || data.crLfMatcher
                .isLineFeed(this.byteBuffer, this.endBuffer)) {
              this.endBuffer++;
              this.currentOffset++;
              this.totalBytesRead++;
              if (checkBufferSize()) {
                break; // nothing more to read.
              }
            }
          }

          // Make sure we start at the right position the next time around.
          this.startBuffer = this.endBuffer;
        }

        //            incrementLinesInput();
        if (conversionExceptions != null && conversionExceptions.size() > 0) {
          // Forward the first exception
          throw new KettleConversionException(
              "There were " + conversionExceptions.size() + " conversion errors on line ",
              conversionExceptions, exceptionFields, outputRowData);
        }
        if (outputIndex > 0 && outputIndex < meta.getInputFields().length) {
          badRecordslogger.addBadRecordsToBilder(outputRowData, meta.getInputFields().length,
              "Row record is not in valid csv format.", null);
          continue;
        } else {
          return outputRowData;
        }
      }
    } catch (KettleConversionException e) {
      throw e;
    } catch (Exception e) {
      throw new KettleFileException("Exception reading line using NIO", e);
    }
  }

  /**
   * Check to see if the buffer size is large enough given the data.endBuffer pointer.<br>
   * Resize the buffer if there is not enough room.
   *
   * @return false if everything is OK, true if there is a problem and we should stop.
   * @throws IOException in case there is a I/O problem (read error)
   */
  private boolean checkBufferSize() throws IOException {
    if (this.endBuffer >= this.bufferSize) {
      // Oops, we need to read more data...
      // Better resize this before we read other things in it...
      //
      this.resizeByteBufferArray();

      // Also read another chunk of data, now that we have the space for it...
      //
      int n = this.readBufferFromFile();

      // If we didn't manage to read something, we return true to indicate we're done
      //
      return n < 0;
    }
    return false;
  }

  private int calculateFieldLength(boolean newLineFound, int newLines, boolean enclosureFound,
      boolean endOfBuffer) {

    int length = this.endBuffer - this.startBuffer;
    if (newLineFound) {
      length -= newLines;
      if (length <= 0) {
        length = 0;
      }
      if (endOfBuffer) {
        this.startBuffer++; // offset for the enclosure in last field before EOF
      }
    }
    if (enclosureFound) {
      this.startBuffer++;
      length -= 2;
      if (length <= 0) {
        length = 0;
      }
    }
    if (length <= 0) {
      length = 0;
    }
    if (data.encodingType != EncodingType.SINGLE) {
      length--;
    }
    return length;
  }
}
