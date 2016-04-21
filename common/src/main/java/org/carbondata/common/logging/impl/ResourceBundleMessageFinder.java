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

package org.carbondata.common.logging.impl;

import java.util.Locale;
import java.util.ResourceBundle;

import org.carbondata.common.logging.LocaleLogMessageFinder;
import org.carbondata.common.logging.LogEvent;

/**
 * Locale Based Message finder for the Log Events
 */
public class ResourceBundleMessageFinder implements LocaleLogMessageFinder {

  private static final String LOG_BUNDLE_NAME = "LogResource";

  public ResourceBundleMessageFinder() {

  }

  public String findLogEventMessage(LogEvent event) {
    return findLogEventMessage(Locale.getDefault(), event);
  }

  public String findLogEventMessage(Locale locale, LogEvent event) {
    String message = null;
    try {
      String location = event.getModuleName() + LOG_BUNDLE_NAME;
      ResourceBundle bundle =
          ResourceBundle.getBundle(location, locale, event.getClass().getClassLoader());
      message = bundle.getString(event.getEventCode());

    } catch (NullPointerException e) {
      return null;
    }
    return message;
  }
}