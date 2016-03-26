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

 /* Copyright (c) 2007 Pentaho Corporation.  All rights reserved.
 * This software was developed by Pentaho Corporation and is provided under the terms
 * of the GNU Lesser General Public License, Version 2.1. You may not use
 * this file except in compliance with the license. If you need a copy of the license,
 * please go to http://www.gnu.org/licenses/lgpl-2.1.txt. The Original Code is Pentaho
 * Data Integration.  The Initial Developer is Pentaho Corporation.
 *
 * Software distributed under the GNU Lesser Public License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or  implied. Please refer to
 * the license for the specific language governing your rights and limitations.*/

package org.carbondata.processing.csvreaderstep;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.vfs.FileObject;
import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.MolapCommonConstants;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.processing.util.MolapDataProcessorLogEvent;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.ResultFile;
import org.pentaho.di.core.exception.KettleConversionException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;
import org.pentaho.di.trans.steps.textfileinput.EncodingType;

/**
 * Read a simple CSV file
 * Just output Strings found in the file...
 */
public class CsvInput extends BaseStep implements StepInterface {
    private static final Class<?> PKG = CsvInput.class;
    // for i18n purposes, needed by Translator2!!   $NON-NLS-1$
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(CsvInput.class.getName());
    private CsvInputMeta meta;
    private CsvInputData data;

    public CsvInput(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr,
            TransMeta transMeta, Trans trans) {
        super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
        LOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                "** Using csv file **");
        //System.out.println("****************** Using my csv file");
    }

    /**
     * This method is borrowed from TextFileInput
     *
     * @param log
     * @param line
     * @param delimiter
     * @param enclosure
     * @param escapeCharacter
     * @return
     * @throws KettleException
     */
    public static final String[] guessStringsFromLine(LogChannelInterface log, String line,
            String delimiter, String enclosure, String escapeCharacter) throws KettleException {
        List<String> strings = new ArrayList<String>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        //        int fieldnr;

        String pol; // piece of line

        try {
            if (line == null) {
                return null;
            }

            // Split string in pieces, only for CSV!

            //      fieldnr = 0;
            int pos = 0;
            int length = line.length();
            boolean dencl = false;

            int lenEncl = (enclosure == null ? 0 : enclosure.length());
            int lenEsc = (escapeCharacter == null ? 0 : escapeCharacter.length());

            while (pos < length) {
                int from = pos;
                int next;

                boolean enclFound;
                boolean containsEscapedEnclosures = false;
                boolean containsEscapedSeparators = false;

                // Is the field beginning with an enclosure?
                // "aa;aa";123;"aaa-aaa";000;...
                if (lenEncl > 0 && line.substring(from, from + lenEncl)
                        .equalsIgnoreCase(enclosure)) {
                    if (log.isRowLevel()) {
                        log.logRowlevel(
                                BaseMessages.getString(PKG, "CsvInput.Log.ConvertLineToRowTitle"),
                                BaseMessages.getString(PKG, "CsvInput.Log.ConvertLineToRow",
                                        line.substring(from,
                                                from + lenEncl))); //$NON-NLS-1$ //$NON-NLS-2$
                    }
                    enclFound = true;
                    int p = from + lenEncl;

                    boolean isEnclosure =
                            lenEncl > 0 && p + lenEncl < length && line.substring(p, p + lenEncl)
                                    .equalsIgnoreCase(enclosure);
                    boolean isEscape =
                            lenEsc > 0 && p + lenEsc < length && line.substring(p, p + lenEsc)
                                    .equalsIgnoreCase(escapeCharacter);

                    boolean enclosureAfter = false;

                    // Is it really an enclosure? See if it's not repeated twice or escaped!
                    if ((isEnclosure || isEscape) && p < length - 1) {
                        String strnext = line.substring(p + lenEncl, p + 2 * lenEncl);
                        if (strnext.equalsIgnoreCase(enclosure)) {
                            p++;
                            enclosureAfter = true;
                            dencl = true;

                            // Remember to replace them later on!
                            if (isEscape) {
                                containsEscapedEnclosures = true;
                            }
                        }
                    }

                    // Look for a closing enclosure!
                    while ((!isEnclosure || enclosureAfter) && p < line.length()) {
                        p++;
                        enclosureAfter = false;
                        isEnclosure = lenEncl > 0 && p + lenEncl < length && line
                                .substring(p, p + lenEncl).equals(enclosure);
                        isEscape =
                                lenEsc > 0 && p + lenEsc < length && line.substring(p, p + lenEsc)
                                        .equals(escapeCharacter);

                        // Is it really an enclosure? See if it's not repeated twice or escaped!
                        if ((isEnclosure || isEscape) && p < length - 1) // Is
                        {
                            String strnext = line.substring(p + lenEncl, p + 2 * lenEncl);
                            if (strnext.equals(enclosure)) {
                                p++;
                                enclosureAfter = true;
                                dencl = true;

                                // Remember to replace them later on!
                                if (isEscape) {
                                    containsEscapedEnclosures = true; // remember
                                }
                            }
                        }
                    }

                    if (p >= length) {
                        next = p;
                    } else {
                        next = p + lenEncl;
                    }

                    if (log.isRowLevel()) {
                        log.logRowlevel(
                                BaseMessages.getString(PKG, "CsvInput.Log.ConvertLineToRowTitle"),
                                BaseMessages.getString(PKG, "CsvInput.Log.EndOfEnclosure",
                                        "" + p)); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                    }
                } else {
                    enclFound = false;
                    boolean found = false;
                    int startpoint = from;
                    int tries = 1;
                    do {
                        next = line.indexOf(delimiter, startpoint);

                        // See if this position is preceded by an escape character.
                        if (lenEsc > 0 && next - lenEsc > 0) {
                            String before = line.substring(next - lenEsc, next);

                            if (escapeCharacter != null && escapeCharacter.equals(before)) {
                                // take the next separator, this one is escaped...
                                startpoint = next + 1;
                                tries++;
                                containsEscapedSeparators = true;
                            } else {
                                found = true;
                            }
                        } else {
                            found = true;
                        }
                    } while (!found && next >= 0);
                }
                if (next == -1) {
                    next = length;
                }

                if (enclFound) {
                    pol = line.substring(from + lenEncl, next - lenEncl);
                    if (log.isRowLevel()) {
                        log.logRowlevel(
                                BaseMessages.getString(PKG, "CsvInput.Log.ConvertLineToRowTitle"),
                                BaseMessages.getString(PKG, "CsvInput.Log.EnclosureFieldFound",
                                        "" + pol)); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                    }
                } else {
                    pol = line.substring(from, next);
                    if (log.isRowLevel()) {
                        log.logRowlevel(
                                BaseMessages.getString(PKG, "CsvInput.Log.ConvertLineToRowTitle"),
                                BaseMessages.getString(PKG, "CsvInput.Log.NormalFieldFound",
                                        "" + pol)); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                    }
                }

                if (dencl) {
                    StringBuilder sbpol = new StringBuilder(pol);
                    int idx = sbpol.indexOf(enclosure + enclosure);
                    while (idx >= 0) {
                        sbpol.delete(idx, idx + (enclosure == null ? 0 : enclosure.length()));
                        idx = sbpol.indexOf(enclosure + enclosure);
                    }
                    pol = sbpol.toString();
                }

                //  replace the escaped enclosures with enclosures...
                if (containsEscapedEnclosures) {
                    String replace = escapeCharacter + enclosure;
                    String replaceWith = enclosure;

                    pol = Const.replace(pol, replace, replaceWith);
                }

                //replace the escaped separators with separators...
                if (containsEscapedSeparators) {
                    String replace = escapeCharacter + delimiter;
                    String replaceWith = delimiter;

                    pol = Const.replace(pol, replace, replaceWith);
                }

                // Now add pol to the strings found!
                strings.add(pol);

                pos = next + delimiter.length();
                //        fieldnr++;
            }
            if (pos == length) {
                if (log.isRowLevel()) {
                    log.logRowlevel(
                            BaseMessages.getString(PKG, "CsvInput.Log.ConvertLineToRowTitle"),
                            BaseMessages.getString(PKG, "CsvInput.Log.EndOfEmptyLineFound"));
                }
                strings.add(""); //$NON-NLS-1$
                //                  fieldnr++;
            }
        } catch (Exception e) {
            throw new KettleException(BaseMessages
                    .getString(PKG, "CsvInput.Log.Error.ErrorConvertingLine", e.toString()),
                    e); //$NON-NLS-1$
        }

        return strings.toArray(new String[strings.size()]);
    }

    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {
        meta = (CsvInputMeta) smi;
        data = (CsvInputData) sdi;

        if (first) {
            first = false;

            data.outputRowMeta = new RowMeta();
            meta.getFields(data.outputRowMeta, getStepname(), null, null, this);

            if (data.filenames == null) {
                // We're expecting the list of filenames from the previous step(s)...
                //
                getFilenamesFromPreviousSteps();
            }

            // We only run in parallel if we have at least one file to process
            // AND if we have more than one step copy running...
            //
            data.parallel = meta.isRunningInParallel() && data.totalNumberOfSteps > 1;

            // The conversion logic for when the lazy conversion is turned of is simple:
            // Pretend it's a lazy conversion object anyway and get the native type during
            // conversion.
            //
            data.convertRowMeta = data.outputRowMeta.clone();
            for (ValueMetaInterface valueMeta : data.convertRowMeta.getValueMetaList()) {
                valueMeta.setStorageType(ValueMetaInterface.STORAGE_TYPE_BINARY_STRING);
            }

            // Calculate the indexes for the filename and row number fields
            //
            data.filenameFieldIndex = -1;
            if (!Const.isEmpty(meta.getFilenameField()) && meta.isIncludingFilename()) {
                data.filenameFieldIndex = meta.getInputFields().length;
            }

            data.rownumFieldIndex = -1;
            if (!Const.isEmpty(meta.getRowNumField())) {
                data.rownumFieldIndex = meta.getInputFields().length;
                if (data.filenameFieldIndex >= 0) {
                    data.rownumFieldIndex++;
                }
            }

            // Now handle the parallel reading aspect: determine total of all the file sizes
            // Then skip to the appropriate file and location in the file to start reading...
            // Also skip to right after the first newline
            //
            if (data.parallel) {
                prepareToRunInParallel();
            }

            // Open the next file...
            //
            if (!openNextFile()) {
                setOutputDone();
                return false; // nothing to see here, move along...
            }
        }

        try {
            Object[] outputRowData = readOneRow(true);    // get row, set busy!
            if (outputRowData == null)  // no more input to be expected...
            {
                if (openNextFile()) {
                    return true; // try again on the next loop...
                } else {
                    setOutputDone(); // last file, end here
                    return false;
                }
            } else {
                incrementLinesRead();
                putRow(data.outputRowMeta,
                        outputRowData);     // copy row to possible alternate rowset(s).
                verifyRejectionRates();
                if (checkFeedback(getLinesInput())) {
                    if (log.isBasic()) {
                        logBasic(BaseMessages.getString(PKG, "CsvInput.Log.LineNumber",
                                Long.toString(getLinesInput()))); //$NON-NLS-1$
                    }
                }
            }
        } catch (KettleConversionException e) {
            if (getStepMeta().isDoingErrorHandling()) {
                StringBuffer errorDescriptions = new StringBuffer(100);
                StringBuffer errorFields = new StringBuffer(50);
                for (int i = 0; i < e.getCauses().size(); i++) {
                    if (i > 0) {
                        errorDescriptions.append(", "); //$NON-NLS-1$
                        errorFields.append(", "); //$NON-NLS-1$
                    }
                    errorDescriptions.append(e.getCauses().get(i).getMessage());
                    errorFields.append(e.getFields().get(i).toStringMeta());
                }

                putError(data.outputRowMeta, e.getRowData(), e.getCauses().size(),
                        errorDescriptions.toString(), errorFields.toString(),
                        "CSVINPUT001"); //$NON-NLS-1$
            } else {
                // Only forward the first cause.
                //
                throw new KettleException(e.getMessage(), e.getCauses().get(0));
            }
        }

        return true;
    }

    private void prepareToRunInParallel() throws KettleException {
    }

    private void getFilenamesFromPreviousSteps() throws KettleException {
        List<String> filenames = new ArrayList<String>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        boolean firstRow = true;
        int index = -1;
        Object[] row = getRow();
        while (row != null) {

            if (firstRow) {
                firstRow = false;

                // Get the filename field index...
                //
                String filenameField = environmentSubstitute(meta.getFilenameField());
                index = getInputRowMeta().indexOfValue(filenameField);
                if (index < 0) {
                    throw new KettleException(BaseMessages
                            .getString(PKG, "CsvInput.Exception.FilenameFieldNotFound",
                                    filenameField)); //$NON-NLS-1$
                }
            }

            String filename = getInputRowMeta().getString(row, index);
            filenames.add(filename);  // add it to the list...

            row = getRow(); // Grab another row...
        }

        data.filenames = filenames.toArray(new String[filenames.size()]);
        logBasic(BaseMessages.getString(PKG, "CsvInput.Log.ReadingFromNrFiles",
                Integer.toString(data.filenames.length))); //$NON-NLS-1$
    }

    @Override
    public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
        try {
            // Close the previous file...
            //
            if (data.bufferedInputStream != null) {
                data.bufferedInputStream.close();
            }
        } catch (Exception e) {
            logError("Error closing file channel", e);
        }

        super.dispose(smi, sdi);
    }

    protected boolean openNextFile() throws KettleException {
        try {

            // Close the previous file...
            //
            if (data.bufferedInputStream != null) {
                data.bufferedInputStream.close();
            }

            if (data.filenr >= data.filenames.length) {
                return false;
            }

            // Open the next one...
            //
            FileObject fileObject =
                    KettleVFS.getFileObject(data.filenames[data.filenr], getTransMeta());

            if (meta.isLazyConversionActive()) {
                data.binaryFilename =
                        data.filenames[data.filenr].getBytes(Charset.defaultCharset());
            }

            initializeFileReader(fileObject);

            // Add filename to result filenames ?
            if (meta.isAddResultFile()) {
                ResultFile resultFile = new ResultFile(ResultFile.FILE_TYPE_GENERAL, fileObject,
                        getTransMeta().getName(), toString());
                resultFile.setComment("File was read by a Csv input step");
                addResultFile(resultFile);
            }

            // Move to the next filename
            //
            data.filenr++;

            // See if we need to skip a row...
            // - If you have a header row checked and if you're not running in parallel
            // - If you're running in parallel, if a header row is checked, if you're at
            // the beginning of a file
            //
            if (meta.isHeaderPresent()) {
                if ((!data.parallel) || // Standard flat file : skip header
                        (data.parallel && data.bytesToSkipInFirstFile <= 0)) {
                    readHeader(); // skip this row.
                    logBasic(BaseMessages.getString(PKG, "CsvInput.Log.HeaderRowSkipped",
                            data.filenames[data.filenr - 1])); //$NON-NLS-1$
                }
            }

            // Reset the row number pointer...
            //
            data.rowNumber = 1L;

            // Don't skip again in the next file...
            //
            data.bytesToSkipInFirstFile = -1L;

            return true;
        } catch (KettleException e) {
            throw e;
        } catch (Exception e) {
            throw new KettleException(e);
        }
    }

    public void readHeader() throws KettleException {
        readOneRow(false);
    }

    protected void initializeFileReader(FileObject fileObject) throws IOException {
        String filePath = KettleVFS.getFilename(fileObject);
        data.bufferedInputStream = FileFactory.getDataInputStream(KettleVFS.getFilename(fileObject),
                FileFactory.getFileType(filePath), data.preferredBufferSize);
    }

    /**
     * Check to see if the buffer size is large enough given the data.endBuffer pointer.<br>
     * Resize the buffer if there is not enough room.
     *
     * @return false if everything is OK, true if there is a problem and we should stop.
     * @throws IOException in case there is a I/O problem (read error)
     */
    private boolean checkBufferSize() throws IOException {
        if (data.endBuffer >= data.bufferSize) {
            // Oops, we need to read more data...
            // Better resize this before we read other things in it...
            //
            data.resizeByteBufferArray();

            // Also read another chunk of data, now that we have the space for it...
            //
            int n = data.readBufferFromFile();

            // If we didn't manage to read something, we return true to indicate we're done
            //
            return n < 0;
        }
        return false;
    }

    /**
     * Read a single row of data from the file...
     *
     * @param doConversions if you want to do conversions, set to false for the header row.
     * @return a row of data...
     * @throws KettleException
     */
    private Object[] readOneRow(boolean doConversions) throws KettleException {

        try {

            Object[] outputRowData = RowDataUtil
                    .allocateRowData(data.outputRowMeta.size() - RowDataUtil.OVER_ALLOCATE_SIZE);
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
                    //	}

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
                            .matchesPattern(data.byteBuffer, data.endBuffer, data.delimiter)) {
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
                        //
                            (!meta.isNewlinePossibleInFields()
                                    || outputIndex == meta.getInputFields().length - 1) && (
                                    data.crLfMatcher.isReturn(data.byteBuffer, data.endBuffer)
                                            || data.crLfMatcher
                                            .isLineFeed(data.byteBuffer, data.endBuffer))) {

                        if (data.encodingType.equals(EncodingType.DOUBLE_LITTLE_ENDIAN)
                                || data.encodingType.equals(EncodingType.DOUBLE_BIG_ENDIAN)) {
                            data.endBuffer += 2;
                        } else {
                            data.endBuffer++;
                        }

                        data.totalBytesRead++;
                        newLines = 1;

                        if (data.endBuffer >= data.bufferSize) {
                            // Oops, we need to read more data...
                            // Better resize this before we read other things in it...
                            //
                            data.resizeByteBufferArray();

                            // Also read another chunk of data, now that we have the space for it...
                            // Ignore EOF, there might be other stuff in the buffer.
                            //
                            data.readBufferFromFile();
                        }

                        // re-check for double delimiters...
                        if (data.crLfMatcher.isReturn(data.byteBuffer, data.endBuffer)
                                || data.crLfMatcher.isLineFeed(data.byteBuffer, data.endBuffer)) {
                            data.endBuffer++;
                            data.totalBytesRead++;
                            newLines = 2;
                            if (data.endBuffer >= data.bufferSize) {
                                // Oops, we need to read more data...
                                // Better resize this before we read other things in it...
                                //
                                data.resizeByteBufferArray();

                                // Also read another chunk of data, now that we have the space for
                                // it...
                                // Ignore EOF, there might be other stuff in the buffer.
                                //
                                data.readBufferFromFile();
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
                            .matchesPattern(data.byteBuffer, data.endBuffer, data.enclosure)) {

                        enclosureFound = true;
                        boolean keepGoing;
                        do {
                            if (data.increaseEndBuffer()) {
                                enclosureFound = false;
                                break;
                            }
                            keepGoing = !data.enclosureMatcher
                                    .matchesPattern(data.byteBuffer, data.endBuffer,
                                            data.enclosure);
                            if (!keepGoing) {
                                // We found an enclosure character.
                                // Read another byte...
                                if (data.increaseEndBuffer()) {
                                    enclosureFound = false;
                                    break;
                                }

                                // If this character is also an enclosure, we can consider the
                                // enclosure "escaped".
                                // As such, if this is an enclosure, we keep going...
                                //
                                keepGoing = data.enclosureMatcher
                                        .matchesPattern(data.byteBuffer, data.endBuffer,
                                                data.enclosure);
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
                                            .matchesPattern(data.byteBuffer, data.endBuffer,
                                                    data.delimiter) || data.crLfMatcher
                                            .isReturn(data.byteBuffer, data.endBuffer)
                                            || data.crLfMatcher
                                            .isLineFeed(data.byteBuffer, data.endBuffer));
                                }

                            }
                        } while (keepGoing);

                        // Did we reach the end of the buffer?
                        //
                        if (data.endBuffer >= data.bufferSize) {
                            newLineFound = true; // consider it a newline to break out of the upper
                            // while loop
                            newLines += 2; // to remove the enclosures in case of missing
                            // newline on last line.
                            endOfBuffer = true;
                            break;
                        }
                    } else {

                        data.endBuffer++;
                        data.totalBytesRead++;

                        if (checkBufferSize()) {
                            if (data.endBuffer >= data.bufferSize) {
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
                int length =
                        calculateFieldLength(newLineFound, newLines, enclosureFound, endOfBuffer);

                byte[] field = new byte[length];
                System.arraycopy(data.byteBuffer, data.startBuffer, field, 0, length);

                // Did we have any escaped characters in there?
                //
                if (escapedEnclosureFound > 0) {
                    if (log.isRowLevel()) {
                        logRowlevel("Escaped enclosures found in " + new String(field,
                                Charset.defaultCharset()));
                    }
                    field = data.removeEscapedEnclosures(field, escapedEnclosureFound);
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
                        ValueMetaInterface sourceValueMeta =
                                data.convertRowMeta.getValueMeta(outputIndex);
                        try {
                            outputRowData[outputIndex++] =
                                    sourceValueMeta.convertBinaryStringToNativeType(field);
                        } catch (KettleValueException e) {
                            // There was a conversion error,
                            //
                            outputRowData[outputIndex++] = null;

                            if (conversionExceptions == null) {
                                conversionExceptions = new ArrayList<Exception>(
                                        MolapCommonConstants.CONSTANT_SIZE_TEN);
                                exceptionFields = new ArrayList<ValueMetaInterface>(
                                        MolapCommonConstants.CONSTANT_SIZE_TEN);
                            }

                            conversionExceptions.add(e);
                            exceptionFields.add(sourceValueMeta);
                        }
                    }
                } else {
                    outputRowData[outputIndex++] =
                            null; // nothing for the header, no conversions here.
                }

                // OK, move on to the next field...
                if (!newLineFound) {
                    data.endBuffer++;
                    data.totalBytesRead++;
                }
                data.startBuffer = data.endBuffer;
            }

            // See if we reached the end of the line.
            // If not, we need to skip the remaining items on the line until the next newline...
            //
            if (!newLineFound && !checkBufferSize()) {
                do {
                    data.endBuffer++;
                    data.totalBytesRead++;

                    if (checkBufferSize()) {
                        break; // nothing more to read.
                    }

                    // HANDLE: if we're using quoting we might be dealing with a very dirty file
                    // with quoted newlines in trailing fields. (imagine that)
                    // In that particular case we want to use the same logic we use above
                    // (refactored a bit) to skip these fields.

                } while (!data.crLfMatcher.isReturn(data.byteBuffer, data.endBuffer)
                        && !data.crLfMatcher.isLineFeed(data.byteBuffer, data.endBuffer));

                if (!checkBufferSize()) {
                    while (data.crLfMatcher.isReturn(data.byteBuffer, data.endBuffer)
                            || data.crLfMatcher.isLineFeed(data.byteBuffer, data.endBuffer)) {
                        data.endBuffer++;
                        data.totalBytesRead++;
                        if (checkBufferSize()) {
                            break; // nothing more to read.
                        }
                    }
                }

                // Make sure we start at the right position the next time around.
                data.startBuffer = data.endBuffer;
            }

            // Optionally add the current filename to the mix as well...
            //
            if (meta.isIncludingFilename() && !Const.isEmpty(meta.getFilenameField())) {
                if (meta.isLazyConversionActive()) {
                    outputRowData[data.filenameFieldIndex] = data.binaryFilename;
                } else {
                    outputRowData[data.filenameFieldIndex] = data.filenames[data.filenr - 1];
                }
            }

            addRowDetails(outputRowData);

            incrementLinesInput();
            if (conversionExceptions != null && conversionExceptions.size() > 0) {
                // Forward the first exception
                //
                throw new KettleConversionException(
                        "There were " + conversionExceptions.size() + " conversion errors on line "
                                + getLinesInput(), conversionExceptions, exceptionFields,
                        outputRowData);
            }

            return outputRowData;
        } catch (KettleConversionException e) {
            throw e;
        } catch (Exception e) {
            throw new KettleFileException("Exception reading line using NIO", e);
        }

    }

    protected void addRowDetails(Object[] outputRowData) {
        if (data.isAddingRowNumber) {
            outputRowData[data.rownumFieldIndex] = Long.valueOf(data.rowNumber++);
        }
    }

    private int calculateFieldLength(boolean newLineFound, int newLines, boolean enclosureFound,
            boolean endOfBuffer) {

        int length = data.endBuffer - data.startBuffer;
        if (newLineFound) {
            length -= newLines;
            if (length <= 0) {
                length = 0;
            }
            if (endOfBuffer) {
                data.startBuffer++; // offset for the enclosure in last field before EOF
            }
        }
        if (enclosureFound) {
            data.startBuffer++;
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

    public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
        meta = (CsvInputMeta) smi;
        data = (CsvInputData) sdi;

        if (super.init(smi, sdi)) {
            data.preferredBufferSize =
                    Integer.parseInt(environmentSubstitute(meta.getBufferSize()));

            // If the step doesn't have any previous steps, we just get the filename.
            // Otherwise, we'll grab the list of filenames later...
            //
            if (getTransMeta().findNrPrevSteps(getStepMeta()) == 0) {
                String filename = environmentSubstitute(meta.getFilename());

                if (Const.isEmpty(filename)) {
                    logError(BaseMessages
                            .getString(PKG, "CsvInput.MissingFilename.Message")); //$NON-NLS-1$
                    return false;
                }

                data.filenames = new String[] { filename, };
            } else {
                data.filenames = null;
                data.filenr = 0;
            }

            data.totalBytesRead = 0L;

            data.encodingType = EncodingType.guessEncodingType(meta.getEncoding());

            // PDI-2489 - set the delimiter byte value to the code point of the
            // character as represented in the input file's encoding
            try {
                data.delimiter = data.encodingType
                        .getBytes(environmentSubstitute(meta.getDelimiter()), meta.getEncoding());

                if (Const.isEmpty(meta.getEnclosure())) {
                    data.enclosure = null;
                } else {
                    data.enclosure = data.encodingType
                            .getBytes(environmentSubstitute(meta.getEnclosure()),
                                    meta.getEncoding());
                }

            } catch (UnsupportedEncodingException e) {
                logError(BaseMessages.getString(PKG, "CsvInput.BadEncoding.Message"),
                        e); //$NON-NLS-1$
                return false;
            }

            data.isAddingRowNumber = !Const.isEmpty(meta.getRowNumField());

            // Handle parallel reading capabilities...
            //

            if (meta.isRunningInParallel()) {
                data.totalNumberOfSteps = getUniqueStepCountAcrossSlaves();

                // We are not handling a single file, but possibly a list of files...
                // As such, the fair thing to do is calculate the total size of the files
                // Then read the required block.
                //

            }

            // Set the most efficient pattern matcher to match the delimiter.
            //
            if (data.delimiter.length == 1) {
                data.delimiterMatcher = new SingleBytePatternMatcher();
            } else {
                data.delimiterMatcher = new MultiBytePatternMatcher();
            }

            // Set the most efficient pattern matcher to match the enclosure.
            //
            if (data.enclosure == null) {
                data.enclosureMatcher = new EmptyPatternMatcher();
            } else {
                if (data.enclosure.length == 1) {
                    data.enclosureMatcher = new SingleBytePatternMatcher();
                } else {
                    data.enclosureMatcher = new MultiBytePatternMatcher();
                }
            }

            switch (data.encodingType) {
            case DOUBLE_BIG_ENDIAN:
                data.crLfMatcher = new MultiByteBigCrLfMatcher();
                break;
            case DOUBLE_LITTLE_ENDIAN:
                data.crLfMatcher = new MultiByteLittleCrLfMatcher();
                break;
            default:
                data.crLfMatcher = new SingleByteCrLfMatcher();
                break;
            }

            return true;

        }
        return false;
    }

    public void closeFile() throws KettleException {

        try {
            if (data.bufferedInputStream != null) {
                data.bufferedInputStream.close();
            }
        } catch (IOException e) {
            throw new KettleException(
                    "Unable to close file channel for file '" + data.filenames[data.filenr - 1], e);
        }
    }

    public boolean isWaitingForData() {
        return true;
    }
}