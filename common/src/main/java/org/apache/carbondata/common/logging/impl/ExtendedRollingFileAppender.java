/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.common.logging.impl;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.log4j.Level;
import org.apache.log4j.RollingFileAppender;
import org.apache.log4j.helpers.CountingQuietWriter;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.LoggingEvent;

/**
 * Copied from log4j to remove the hard coding for the file name from it Copied
 * form log4j and modified for renaming files
 */
public class ExtendedRollingFileAppender extends RollingFileAppender {

  private static final String DATE_FORMAT_FOR_TRANSFER = "yyyy-MM-dd'_'HH-mm-ss";
  protected int currentLevel = Level.FATAL_INT;
  /**
   * Now in log file rolling(after file size
   * exceeded the threshold) and deletion (after file count exceeded the file
   * count threshold) it will print log message
   */

  private long nextRollover = 0;
  private volatile boolean cleanupInProgress = false;

  /**
   * Total number of files at any point of time should be Backup number of
   * files + current file
   */
  private static void cleanLogs(final String startName, final String folderPath,
      int maxBackupIndex) {
    final String fileStartName = startName.toLowerCase(Locale.US);
    // Delete the oldest file, to keep Windows happy.
    File file = new File(folderPath);

    if (file.exists()) {
      File[] files = file.listFiles(new FileFilter() {

        public boolean accept(File file) {
          if (!file.isDirectory() && file.getName().toLowerCase(Locale.US)
              .startsWith(fileStartName)) {
            return true;
          }
          return false;
        }
      });

      if (null == files) {
        return;
      }

      int backupFiles = files.length - 1;

      if (backupFiles <= maxBackupIndex) {
        return;
      }

      // Sort the file based on its name.
      TreeMap<String, File> sortedMap = new TreeMap<String, File>();
      for (File file1 : files) {
        sortedMap.put(file1.getName(), file1);
      }

      // Remove the first log file from map. it will be <startName>.log
      // itself which will be backed up in rollover
      sortedMap.remove(sortedMap.firstKey());

      Iterator<Entry<String, File>> it = sortedMap.entrySet().iterator();
      Entry<String, File> temp = null;

      // After clean up the files should be maxBackupIndex -1 number of
      // files. Because one more backup file
      // will be created after this method call is over
      while (it.hasNext() && backupFiles > maxBackupIndex) {
        temp = it.next();
        File deleteFile = temp.getValue();
        // Delete the file
        // After deletion of log file it
        // will print the log message in ReportService.log
        if (deleteFile.delete()) {
          backupFiles--;
        } else {
          LogLog.error("Couldn't delete file :: " + deleteFile.getPath());
        }
      }
    }
  }

  /**
   * Copied from log4j to remove hardcoding of file name
   */
  public void rollOver() {
    File target;
    File file = new File(fileName);

    String fileStartName = file.getName();
    int dotIndex = fileStartName.indexOf('.');

    if (dotIndex != -1) {
      fileStartName = fileStartName.substring(0, dotIndex);
    }
    final String startName = fileStartName;
    final String folderPath = file.getParent();

    if (qw != null) {
      long size = ((CountingQuietWriter) qw).getCount();
      LogLog.debug("rolling over count=" + size);
      // if operation fails, do not roll again until
      // maxFileSize more bytes are written
      nextRollover = size + maxFileSize;
    }

    LogLog.debug("maxBackupIndex=" + maxBackupIndex);

    boolean renameSucceeded = true;

    // If maxBackups <= 0, then there is no file renaming to be done.
    if (maxBackupIndex > 0) {
      DateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT_FOR_TRANSFER);

      StringBuilder buffer = new StringBuilder();
      String extension = "";
      if (fileName.contains(".")) {
        extension = fileName.substring(fileName.lastIndexOf("."));
        buffer.append(fileName.substring(0, fileName.lastIndexOf(".")));
      } else {
        buffer.append(fileName);
      }
      buffer.append("_").append(dateFormat.format(new Date())).append(extension);
      // Rename fileName to fileName.1
      target = new File(buffer.toString());

      this.closeFile(); // keep windows happy.

      LogLog.debug("Renaming file " + file + " to " + target);
      renameSucceeded = file.renameTo(target);

      //
      // if file rename failed, reopen file with append = true
      //
      if (!renameSucceeded) {
        try {
          this.setFile(fileName, true, bufferedIO, bufferSize);
        } catch (InterruptedIOException e) {
          Thread.currentThread().interrupt();
        } catch (IOException e) {
          LogLog.error("setFile(" + fileName + ", true) call failed.", e);
        }
      }
    }

    //
    // if all renames were successful, then
    //
    if (renameSucceeded) {
      try {
        // This will also close the file. This is OK since multiple
        // close operations are safe.
        this.setFile(fileName, false, bufferedIO, bufferSize);
        nextRollover = 0;
      } catch (InterruptedIOException e) {
        Thread.currentThread().interrupt();
      } catch (IOException e) {
        LogLog.error("setFile(" + fileName + ", false) call failed.", e);
      }
    }

    // Do clean up finally
    if (!cleanupInProgress) {
      cleanUpLogs(startName, folderPath);
    }
  }

  private void cleanUpLogs(final String startName, final String folderPath) {
    if (maxBackupIndex > 0) {
      // Clean the logs files
      Runnable r = new Runnable() {

        public void run() {
          synchronized (ExtendedRollingFileAppender.class) {
            cleanupInProgress = true;
            try {
              cleanLogs(startName, folderPath, maxBackupIndex);
            } catch (Throwable e) {
              // ignore any error
              LogLog.error("Cleaning logs failed", e);
            } finally {
              cleanupInProgress = false;
            }
          }
        }
      };

      Thread t = new Thread(r);
      t.start();
    }
  }

  protected void subAppend(LoggingEvent event) {
    if (event.getLevel().toInt() <= currentLevel) {
      super.subAppend(event);
      if (fileName != null && qw != null) {
        long size = ((CountingQuietWriter) qw).getCount();
        if (size >= maxFileSize && size >= nextRollover) {
          rollOver();
        }
      }
    }
  }

}
