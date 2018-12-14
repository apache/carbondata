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

package org.apache.carbondata.core.util;

public class Property<T> {

  private String name;
  private T value;
  private T defaultValue;
  private T minValue;
  private T maxValue;
  private String doc;
  private boolean dynamicConfigurable;

  public Property(String name, T defaultValue, String doc, boolean dynamicConfigurable) {
    this.name = name;
    this.defaultValue = defaultValue;
    this.doc = doc;
    this.dynamicConfigurable = dynamicConfigurable;
  }

  public Property(String name, T defaultValue, T minValue,
                  T maxValue, String doc, boolean dynamicConfigurable) {
    this.name = name;
    this.defaultValue = defaultValue;
    this.minValue = minValue;
    this.maxValue = maxValue;
    this.doc = doc;
    this.dynamicConfigurable = dynamicConfigurable;
  }

  public String getName() {
    return name;
  }

  public T getValue() {
    return value;
  }

  public void setValue(T value) {
    this.value = value;
  }

  public T getDefaultValue() {
    return defaultValue;
  }

  public T getMinValue() {
    return (T) minValue;
  }

  public int getMinValueInt() {
    return Integer.parseInt(String.valueOf(minValue));
  }

  public double getMinValueDouble() {
    return Double.parseDouble(String.valueOf(minValue));
  }

  public T getMaxValue() {
    return (T) maxValue;
  }

  public int getMaxValueInt() {
    return Integer.parseInt(String.valueOf(maxValue));
  }

  public double getMaxValueDouble() {
    return Double.parseDouble(String.valueOf(maxValue));
  }

  public String getDefaultValueString() {
    return String.valueOf(defaultValue);
  }

  public int getDefaultValueInt() {
    return Integer.parseInt(String.valueOf(defaultValue));
  }

  public long getDefaultValueLong() {
    return Long.parseLong(String.valueOf(defaultValue));
  }

  public boolean getDefaultValueBoolean() {
    return Boolean.parseBoolean(String.valueOf(defaultValue));
  }

  public String getDoc() {
    return doc;
  }

  public boolean isDynamicConfigurable() {
    return dynamicConfigurable;
  }

  public static PropertyBuilder<String> buildStringProperty() {
    return new PropertyBuilder<String>();
  }

  public static PropertyBuilder<Boolean> buildBooleanProperty() {
    return new PropertyBuilder<Boolean>();
  }

  public static PropertyBuilder<Integer> buildIntProperty() {
    return new PropertyBuilder<Integer>();
  }

  public static PropertyBuilder<Long> buildLongProperty() {
    return new PropertyBuilder<Long>();
  }

  public static PropertyBuilder<Double> buildDoubleProperty() {
    return new PropertyBuilder<Double>();
  }

  @Override
  public String toString() {
    return name;
  }
}