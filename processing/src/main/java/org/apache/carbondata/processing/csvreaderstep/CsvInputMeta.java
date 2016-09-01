/**
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.vfs.FileObject;
import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Counter;
import org.pentaho.di.core.KettleAttributeInterface;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.resource.ResourceDefinition;
import org.pentaho.di.resource.ResourceEntry;
import org.pentaho.di.resource.ResourceEntry.ResourceType;
import org.pentaho.di.resource.ResourceNamingInterface;
import org.pentaho.di.resource.ResourceReference;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInjectionMetaEntry;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInjectionInterface;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.textfileinput.InputFileMetaInterface;
import org.pentaho.di.trans.steps.textfileinput.TextFileInputField;
import org.pentaho.di.trans.steps.textfileinput.TextFileInputMeta;
import org.w3c.dom.Node;

public class CsvInputMeta extends BaseStepMeta
    implements StepMetaInterface, InputFileMetaInterface, StepMetaInjectionInterface {
  private static final Class<?> PKG = CsvInput.class;

  private String filename;

  private String filenameField;

  private boolean includingFilename;

  private String rowNumField;

  private boolean headerPresent;

  private String delimiter;
  private String enclosure;
  private boolean escapeEnclosure;

  private String bufferSize;

  private boolean lazyConversionActive;

  private TextFileInputField[] inputFields;

  private boolean isaddresult;

  private boolean runningInParallel;

  private String encoding;

  private boolean newlinePossibleInFields;

  private String blocksID;

  private String partitionID;

  private String escapeCharacter;

  private String quoteCharacter;

  private String commentCharacter;

  private String maxColumns;

  public CsvInputMeta() {
    super(); // allocate BaseStepMeta
    allocate(0);
  }

  public void loadXML(Node stepnode, List<DatabaseMeta> databases, Map<String, Counter> counters)
      throws KettleXMLException {
    readData(stepnode);
  }

  public void setDefault() {
    delimiter = ",";
    enclosure = "\"";
    headerPresent = true;
    lazyConversionActive = true;
    isaddresult = false;
    bufferSize = "50000";
    blocksID = "";
    partitionID = "";
    escapeCharacter ="\\";
    quoteCharacter = "\"";
    commentCharacter = "#";
  }

  private void readData(Node stepnode) throws KettleXMLException {
    try {
      filename = XMLHandler.getTagValue(stepnode, getXmlCode("FILENAME"));
      filenameField = XMLHandler.getTagValue(stepnode, getXmlCode("FILENAME_FIELD"));
      rowNumField = XMLHandler.getTagValue(stepnode, getXmlCode("ROW_NUM_FIELD"));
      includingFilename =
          "Y".equalsIgnoreCase(XMLHandler.getTagValue(stepnode, getXmlCode("INCLUDE_FILENAME")));
      delimiter = XMLHandler.getTagValue(stepnode, getXmlCode("DELIMITER"));
      enclosure = XMLHandler.getTagValue(stepnode, getXmlCode("ENCLOSURE"));
      escapeEnclosure =
          "Y".equalsIgnoreCase(XMLHandler.getTagValue(stepnode, getXmlCode("ESACAPE_ENCLOSURE")));
      bufferSize = XMLHandler.getTagValue(stepnode, getXmlCode("BUFFERSIZE"));
      headerPresent =
          "Y".equalsIgnoreCase(XMLHandler.getTagValue(stepnode, getXmlCode("HEADER_PRESENT")));
      lazyConversionActive =
          "Y".equalsIgnoreCase(XMLHandler.getTagValue(stepnode, getXmlCode("LAZY_CONVERSION")));
      isaddresult =
          "Y".equalsIgnoreCase(XMLHandler.getTagValue(stepnode, getXmlCode("ADD_FILENAME_RESULT")));
      runningInParallel =
          "Y".equalsIgnoreCase(XMLHandler.getTagValue(stepnode, getXmlCode("PARALLEL")));
      String nlp = XMLHandler.getTagValue(stepnode, getXmlCode("NEWLINE_POSSIBLE"));
      if (Const.isEmpty(nlp)) {
        if (runningInParallel) {
          newlinePossibleInFields = false;
        } else {
          newlinePossibleInFields = true;
        }
      } else {
        newlinePossibleInFields = "Y".equalsIgnoreCase(nlp);
      }
      encoding = XMLHandler.getTagValue(stepnode, getXmlCode("ENCODING"));
      blocksID = XMLHandler.getTagValue(stepnode, "blocksID");
      partitionID = XMLHandler.getTagValue(stepnode, "partitionID");
      escapeCharacter = XMLHandler.getTagValue(stepnode, "escapeCharacter");
      quoteCharacter = XMLHandler.getTagValue(stepnode, "quoteCharacter");
      commentCharacter = XMLHandler.getTagValue(stepnode, "commentCharacter");
      maxColumns = XMLHandler.getTagValue(stepnode, "maxColumns");
      Node fields = XMLHandler.getSubNode(stepnode, getXmlCode("FIELDS"));
      int nrfields = XMLHandler.countNodes(fields, getXmlCode("FIELD"));

      allocate(nrfields);

      for (int i = 0; i < nrfields; i++) {
        inputFields[i] = new TextFileInputField();

        Node fnode = XMLHandler.getSubNodeByNr(fields, getXmlCode("FIELD"), i);

        inputFields[i].setName(XMLHandler.getTagValue(fnode, getXmlCode("FIELD_NAME")));
        inputFields[i]
            .setType(ValueMeta.getType(XMLHandler.getTagValue(fnode, getXmlCode("FIELD_TYPE"))));
        inputFields[i].setFormat(XMLHandler.getTagValue(fnode, getXmlCode("FIELD_FORMAT")));
        inputFields[i]
            .setCurrencySymbol(XMLHandler.getTagValue(fnode, getXmlCode("FIELD_CURRENCY")));
        inputFields[i].setDecimalSymbol(XMLHandler.getTagValue(fnode,
            getXmlCode("FIELD_DECIMAL")));
        inputFields[i].setGroupSymbol(XMLHandler.getTagValue(fnode, getXmlCode("FIELD_GROUP")));
        inputFields[i]
            .setLength(Const.toInt(XMLHandler.getTagValue(fnode, getXmlCode("FIELD_LENGTH")), -1));
        inputFields[i].setPrecision(
            Const.toInt(XMLHandler.getTagValue(fnode, getXmlCode("FIELD_PRECISION")), -1));
        inputFields[i].setTrimType(ValueMeta
            .getTrimTypeByCode(XMLHandler.getTagValue(fnode, getXmlCode("FIELD_TRIM_TYPE"))));
      }
    } catch (Exception e) {
      throw new KettleXMLException("Unable to load step info from XML", e);
    }
  }

  public void allocate(int nrFields) {
    inputFields = new TextFileInputField[nrFields];
  }

  public String getXML() {
    StringBuffer retval = new StringBuffer(500);

    retval.append("    ").append(XMLHandler.addTagValue(getXmlCode("FILENAME"), filename));
    retval.append("    ")
        .append(XMLHandler.addTagValue(getXmlCode("FILENAME_FIELD"), filenameField));
    retval.append("    ").append(XMLHandler.addTagValue(getXmlCode("ROW_NUM_FIELD"), rowNumField));
    retval.append("    ")
        .append(XMLHandler.addTagValue(getXmlCode("INCLUDE_FILENAME"), includingFilename));
    retval.append("    ").append(XMLHandler.addTagValue(getXmlCode("DELIMITER"), delimiter));
    retval.append("    ").append(XMLHandler.addTagValue(getXmlCode("ENCLOSURE"), enclosure));
    retval.append("    ")
        .append(XMLHandler.addTagValue(getXmlCode("ESACAPE_ENCLOSURE"), escapeEnclosure));
    retval.append("    ")
        .append(XMLHandler.addTagValue(getXmlCode("HEADER_PRESENT"), headerPresent));
    retval.append("    ").append(XMLHandler.addTagValue(getXmlCode("BUFFERSIZE"), bufferSize));
    retval.append("    ")
        .append(XMLHandler.addTagValue(getXmlCode("LAZY_CONVERSION"), lazyConversionActive));
    retval.append("    ")
        .append(XMLHandler.addTagValue(getXmlCode("ADD_FILENAME_RESULT"), isaddresult));
    retval.append("    ").append(XMLHandler.addTagValue(getXmlCode("PARALLEL"), runningInParallel));
    retval.append("    ")
        .append(XMLHandler.addTagValue(getXmlCode("NEWLINE_POSSIBLE"), newlinePossibleInFields));
    retval.append("    ").append(XMLHandler.addTagValue(getXmlCode("ENCODING"), encoding));
    retval.append("    ").append(XMLHandler.addTagValue("blocksID", blocksID));
    retval.append("    ").append(XMLHandler.addTagValue("partitionID", partitionID));
    retval.append("    ").append(XMLHandler.addTagValue("escapeCharacter", escapeCharacter));
    retval.append("    ").append(XMLHandler.addTagValue("quoteCharacter", quoteCharacter));
    retval.append("    ").append(XMLHandler.addTagValue("commentCharacter", commentCharacter));
    retval.append("    ").append(XMLHandler.addTagValue("maxColumns", maxColumns));
    retval.append("    ").append(XMLHandler.openTag(getXmlCode("FIELDS"))).append(Const.CR);
    for (int i = 0; i < inputFields.length; i++) {
      TextFileInputField field = inputFields[i];

      retval.append("      ").append(XMLHandler.openTag(getXmlCode("FIELD"))).append(Const.CR);
      retval.append("        ")
          .append(XMLHandler.addTagValue(getXmlCode("FIELD_NAME"), field.getName()));
      retval.append("        ").append(
          XMLHandler.addTagValue(getXmlCode("FIELD_TYPE"), ValueMeta.getTypeDesc(field.getType())));
      retval.append("        ")
          .append(XMLHandler.addTagValue(getXmlCode("FIELD_FORMAT"), field.getFormat()));
      retval.append("        ")
          .append(XMLHandler.addTagValue(getXmlCode("FIELD_CURRENCY"), field.getCurrencySymbol()));
      retval.append("        ")
          .append(XMLHandler.addTagValue(getXmlCode("FIELD_DECIMAL"), field.getDecimalSymbol()));
      retval.append("        ")
          .append(XMLHandler.addTagValue(getXmlCode("FIELD_GROUP"), field.getGroupSymbol()));
      retval.append("        ")
          .append(XMLHandler.addTagValue(getXmlCode("FIELD_LENGTH"), field.getLength()));
      retval.append("        ")
          .append(XMLHandler.addTagValue(getXmlCode("FIELD_PRECISION"), field.getPrecision()));
      retval.append("        ").append(XMLHandler.addTagValue(getXmlCode("FIELD_TRIM_TYPE"),
          ValueMeta.getTrimTypeCode(field.getTrimType())));
      retval.append("      ").append(XMLHandler.closeTag(getXmlCode("FIELD"))).append(Const.CR);
    }
    retval.append("    ").append(XMLHandler.closeTag(getXmlCode("FIELDS"))).append(Const.CR);

    return retval.toString();
  }

  public void readRep(Repository rep, ObjectId idStep, List<DatabaseMeta> databases,
      Map<String, Counter> counters) throws KettleException {
    try {
      filename = rep.getStepAttributeString(idStep, getRepCode("FILENAME"));
      filenameField = rep.getStepAttributeString(idStep, getRepCode("FILENAME_FIELD"));
      rowNumField = rep.getStepAttributeString(idStep, getRepCode("ROW_NUM_FIELD"));
      includingFilename = rep.getStepAttributeBoolean(idStep, getRepCode("INCLUDE_FILENAME"));
      delimiter = rep.getStepAttributeString(idStep, getRepCode("DELIMITER"));
      enclosure = rep.getStepAttributeString(idStep, getRepCode("ENCLOSURE"));
      escapeEnclosure = rep.getStepAttributeBoolean(idStep, getRepCode("ESACAPE_ENCLOSURE"));
      headerPresent = rep.getStepAttributeBoolean(idStep, getRepCode("HEADER_PRESENT"));
      bufferSize = rep.getStepAttributeString(idStep, getRepCode("BUFFERSIZE"));
      lazyConversionActive = rep.getStepAttributeBoolean(idStep, getRepCode("LAZY_CONVERSION"));
      isaddresult = rep.getStepAttributeBoolean(idStep, getRepCode("ADD_FILENAME_RESULT"));
      runningInParallel = rep.getStepAttributeBoolean(idStep, getRepCode("PARALLEL"));
      newlinePossibleInFields =
          rep.getStepAttributeBoolean(idStep, 0, getRepCode("NEWLINE_POSSIBLE"),
              !runningInParallel);
      encoding = rep.getStepAttributeString(idStep, getRepCode("ENCODING"));
      blocksID = rep.getStepAttributeString(idStep, getRepCode("blocksID"));
      partitionID = rep.getStepAttributeString(idStep, getRepCode("partitionID"));
      escapeCharacter = rep.getStepAttributeString(idStep, getRepCode("escapeCharacter"));
      quoteCharacter = rep.getStepAttributeString(idStep, getRepCode("quoteCharacter"));
      commentCharacter = rep.getStepAttributeString(idStep, getRepCode("commentCharacter"));
      maxColumns = rep.getStepAttributeString(idStep, getRepCode("maxColumns"));
      int nrfields = rep.countNrStepAttributes(idStep, getRepCode("FIELD_NAME"));

      allocate(nrfields);

      for (int i = 0; i < nrfields; i++) {
        inputFields[i] = new TextFileInputField();

        inputFields[i].setName(rep.getStepAttributeString(idStep, i, getRepCode("FIELD_NAME")));
        inputFields[i].setType(
            ValueMeta.getType(rep.getStepAttributeString(idStep, i, getRepCode("FIELD_TYPE"))));
        inputFields[i].setFormat(rep.getStepAttributeString(idStep, i, getRepCode("FIELD_FORMAT")));
        inputFields[i]
            .setCurrencySymbol(rep.getStepAttributeString(idStep, i, getRepCode("FIELD_CURRENCY")));
        inputFields[i]
            .setDecimalSymbol(rep.getStepAttributeString(idStep, i, getRepCode("FIELD_DECIMAL")));
        inputFields[i]
            .setGroupSymbol(rep.getStepAttributeString(idStep, i, getRepCode("FIELD_GROUP")));
        inputFields[i]
            .setLength((int) rep.getStepAttributeInteger(idStep, i, getRepCode("FIELD_LENGTH")));
        inputFields[i].setPrecision(
            (int) rep.getStepAttributeInteger(idStep, i, getRepCode("FIELD_PRECISION")));
        inputFields[i].setTrimType(ValueMeta.getTrimTypeByCode(
            rep.getStepAttributeString(idStep, i, getRepCode("FIELD_TRIM_TYPE"))));
      }
    } catch (Exception e) {
      throw new KettleException("Unexpected error reading step information from the repository", e);
    }
  }

  public void saveRep(Repository rep, ObjectId idTransformation, ObjectId idStep)
      throws KettleException {
    try {
      rep.saveStepAttribute(idTransformation, idStep, getRepCode("FILENAME"), filename);
      rep.saveStepAttribute(idTransformation, idStep, getRepCode("FILENAME_FIELD"), filenameField);
      rep.saveStepAttribute(idTransformation, idStep, getRepCode("ROW_NUM_FIELD"), rowNumField);
      rep.saveStepAttribute(idTransformation, idStep, getRepCode("INCLUDE_FILENAME"),
          includingFilename);
      rep.saveStepAttribute(idTransformation, idStep, getRepCode("DELIMITER"), delimiter);
      rep.saveStepAttribute(idTransformation, idStep, getRepCode("ENCLOSURE"), enclosure);
      rep.saveStepAttribute(idTransformation, idStep, getRepCode("ESACAPE_ENCLOSURE"),
          escapeEnclosure);
      rep.saveStepAttribute(idTransformation, idStep, getRepCode("BUFFERSIZE"), bufferSize);
      rep.saveStepAttribute(idTransformation, idStep, getRepCode("HEADER_PRESENT"), headerPresent);
      rep.saveStepAttribute(idTransformation, idStep, getRepCode("LAZY_CONVERSION"),
          lazyConversionActive);
      rep.saveStepAttribute(idTransformation, idStep, getRepCode("ADD_FILENAME_RESULT"),
          isaddresult);
      rep.saveStepAttribute(idTransformation, idStep, getRepCode("PARALLEL"), runningInParallel);
      rep.saveStepAttribute(idTransformation, idStep, getRepCode("NEWLINE_POSSIBLE"),
          newlinePossibleInFields);
      rep.saveStepAttribute(idTransformation, idStep, getRepCode("ENCODING"), encoding);
      rep.saveStepAttribute(idTransformation, idStep, getRepCode("blocksID"), blocksID);
      rep.saveStepAttribute(idTransformation, idStep, getRepCode("partitionID"), partitionID);
      rep.saveStepAttribute(idTransformation, idStep, getRepCode("escapeCharacter"),
          escapeCharacter);
      rep.saveStepAttribute(idTransformation, idStep, getRepCode("quoteCharacter"),
          quoteCharacter);
      rep.saveStepAttribute(idTransformation, idStep, getRepCode("commentCharacter"),
          commentCharacter);
      rep.saveStepAttribute(idTransformation, idStep, getRepCode("maxColumns"),
          maxColumns);
      for (int i = 0; i < inputFields.length; i++) {
        TextFileInputField field = inputFields[i];

        rep.saveStepAttribute(idTransformation, idStep, i, getRepCode("FIELD_NAME"),
            field.getName());
        rep.saveStepAttribute(idTransformation, idStep, i, getRepCode("FIELD_TYPE"),
            ValueMeta.getTypeDesc(field.getType()));
        rep.saveStepAttribute(idTransformation, idStep, i, getRepCode("FIELD_FORMAT"),
            field.getFormat());
        rep.saveStepAttribute(idTransformation, idStep, i, getRepCode("FIELD_CURRENCY"),
            field.getCurrencySymbol());
        rep.saveStepAttribute(idTransformation, idStep, i, getRepCode("FIELD_DECIMAL"),
            field.getDecimalSymbol());
        rep.saveStepAttribute(idTransformation, idStep, i, getRepCode("FIELD_GROUP"),
            field.getGroupSymbol());
        rep.saveStepAttribute(idTransformation, idStep, i, getRepCode("FIELD_LENGTH"),
            field.getLength());
        rep.saveStepAttribute(idTransformation, idStep, i, getRepCode("FIELD_PRECISION"),
            field.getPrecision());
        rep.saveStepAttribute(idTransformation, idStep, i, getRepCode("FIELD_TRIM_TYPE"),
            ValueMeta.getTrimTypeCode(field.getTrimType()));
      }
    } catch (Exception e) {
      throw new KettleException(
          "Unable to save step information to the repository for id_step=" + idStep, e);
    }
  }

  public void getFields(RowMetaInterface rowMeta, String origin, RowMetaInterface[] info,
      StepMeta nextStep, VariableSpace space) throws KettleStepException {
    rowMeta.clear(); // Start with a clean slate, eats the input

    for (int i = 0; i < inputFields.length; i++) {
      TextFileInputField field = inputFields[i];

      ValueMetaInterface valueMeta = new ValueMeta(field.getName(), field.getType());
      valueMeta.setConversionMask(field.getFormat());
      valueMeta.setLength(field.getLength());
      valueMeta.setPrecision(field.getPrecision());
      valueMeta.setConversionMask(field.getFormat());
      valueMeta.setDecimalSymbol(field.getDecimalSymbol());
      valueMeta.setGroupingSymbol(field.getGroupSymbol());
      valueMeta.setCurrencySymbol(field.getCurrencySymbol());
      valueMeta.setTrimType(field.getTrimType());
      if (lazyConversionActive) {
        valueMeta.setStorageType(ValueMetaInterface.STORAGE_TYPE_BINARY_STRING);
      }
      valueMeta.setStringEncoding(space.environmentSubstitute(encoding));

      // In case we want to convert Strings...
      // Using a copy of the valueMeta object means that the inner and outer representation
      // format is the same.
      // Preview will show the data the same way as we read it.
      // This layout is then taken further down the road by the metadata through the transformation.
      //
      ValueMetaInterface storageMetadata = valueMeta.clone();
      storageMetadata.setType(ValueMetaInterface.TYPE_STRING);
      storageMetadata.setStorageType(ValueMetaInterface.STORAGE_TYPE_NORMAL);
      storageMetadata
          .setLength(-1, -1); // we don't really know the lengths of the strings read in advance.
      valueMeta.setStorageMetadata(storageMetadata);

      valueMeta.setOrigin(origin);

      rowMeta.addValueMeta(valueMeta);
    }

    if (!Const.isEmpty(filenameField) && includingFilename) {
      ValueMetaInterface filenameMeta =
          new ValueMeta(filenameField, ValueMetaInterface.TYPE_STRING);
      filenameMeta.setOrigin(origin);
      if (lazyConversionActive) {
        filenameMeta.setStorageType(ValueMetaInterface.STORAGE_TYPE_BINARY_STRING);
        filenameMeta
            .setStorageMetadata(new ValueMeta(filenameField, ValueMetaInterface.TYPE_STRING));
      }
      rowMeta.addValueMeta(filenameMeta);
    }

    if (!Const.isEmpty(rowNumField)) {
      ValueMetaInterface rowNumMeta = new ValueMeta(rowNumField, ValueMetaInterface.TYPE_INTEGER);
      rowNumMeta.setLength(10);
      rowNumMeta.setOrigin(origin);
      rowMeta.addValueMeta(rowNumMeta);
    }

  }

  public void check(List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepinfo,
      RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info) {
    CheckResult cr;
    if (prev == null || prev.size() == 0) {
      cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK,
          BaseMessages.getString(PKG, "CsvInputMeta.CheckResult.NotReceivingFields"),
          stepinfo); //$NON-NLS-1$
      remarks.add(cr);
    } else {
      cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages
          .getString(PKG, "CsvInputMeta.CheckResult.StepRecevingData", prev.size() + ""),
          stepinfo); //$NON-NLS-1$ //$NON-NLS-2$
      remarks.add(cr);
    }

    // See if we have input streams leading to this step!
    if (input.length > 0) {
      cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR,
          BaseMessages.getString(PKG, "CsvInputMeta.CheckResult.StepRecevingData2"),
          stepinfo); //$NON-NLS-1$
      remarks.add(cr);
    } else {
      cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK,
          BaseMessages.getString(PKG, "CsvInputMeta.CheckResult.NoInputReceivedFromOtherSteps"),
          stepinfo); //$NON-NLS-1$
      remarks.add(cr);
    }
  }

  public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr,
      TransMeta tr, Trans trans) {
    return new CsvInput(stepMeta, stepDataInterface, cnr, tr, trans);
  }

  public StepDataInterface getStepData() {
    return new CsvInputData();
  }

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
   * @return the filename
   */
  public String getFilename() {
    return filename;
  }

  /**
   * @param filename the filename to set
   */
  public void setFilename(String filename) {
    this.filename = filename;
  }

  /**
   * @return the bufferSize
   */
  public String getBufferSize() {
    return bufferSize;
  }

  /**
   * @param bufferSize the bufferSize to set
   */
  public void setBufferSize(String bufferSize) {
    this.bufferSize = bufferSize;
  }

  /**
   * @return true if lazy conversion is turned on: conversions are delayed as long as possible,
   * perhaps to never occur at all.
   */
  public boolean isLazyConversionActive() {
    return lazyConversionActive;
  }

  /**
   * @param lazyConversionActive true if lazy conversion is to be turned on: conversions are
   *                             delayed as long as possible, perhaps to never occur at all.
   */
  public void setLazyConversionActive(boolean lazyConversionActive) {
    this.lazyConversionActive = lazyConversionActive;
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

  @Override
  public List<ResourceReference> getResourceDependencies(TransMeta transMeta, StepMeta stepInfo) {
    List<ResourceReference> references = new ArrayList<ResourceReference>(5);

    ResourceReference reference = new ResourceReference(stepInfo);
    references.add(reference);
    if (!Const.isEmpty(filename)) {
      // Add the filename to the references, including a reference to this
      // step meta data.
      //
      reference.getEntries()
          .add(new ResourceEntry(transMeta.environmentSubstitute(filename), ResourceType.FILE));
    }
    return references;
  }

  /**
   * @return the inputFields
   */
  public TextFileInputField[] getInputFields() {
    return inputFields;
  }

  /**
   * @param inputFields the inputFields to set
   */
  public void setInputFields(TextFileInputField[] inputFields) {
    this.inputFields = inputFields;
  }

  public int getFileFormatTypeNr() {
    return TextFileInputMeta.FILE_FORMAT_MIXED;
  }

  public String[] getFilePaths(VariableSpace space) {
    return new String[] { space.environmentSubstitute(filename), };
  }

  public int getNrHeaderLines() {
    return 1;
  }

  public boolean hasHeader() {
    return isHeaderPresent();
  }

  public String getErrorCountField() {
    return null;
  }

  public String getErrorFieldsField() {
    return null;
  }

  public String getErrorTextField() {
    return null;
  }

  public String getEscapeCharacter() {
    return escapeCharacter;
  }

  public void setEscapeCharacter(String escapeCharacter){
    this.escapeCharacter = escapeCharacter;
  }

  public String getQuoteCharacter() { return quoteCharacter; }

  public void setQuoteCharacter(String quoteCharacter) { this.quoteCharacter = quoteCharacter; }

  public String getCommentCharacter() { return commentCharacter; }

  public void setCommentCharacter(String commentCharacter) {
    this.commentCharacter = commentCharacter;
  }

  public String getFileType() {
    return "CSV";
  }

  public String getSeparator() {
    return delimiter;
  }

  public boolean includeFilename() {
    return false;
  }

  public boolean includeRowNumber() {
    return false;
  }

  public boolean isErrorIgnored() {
    return false;
  }

  public boolean isErrorLineSkipped() {
    return false;
  }

  /**
   * @return the filenameField
   */
  public String getFilenameField() {
    return filenameField;
  }

  /**
   * @param filenameField the filenameField to set
   */
  public void setFilenameField(String filenameField) {
    this.filenameField = filenameField;
  }

  /**
   * @return the includingFilename
   */
  public boolean isIncludingFilename() {
    return includingFilename;
  }

  /**
   * @param includingFilename the includingFilename to set
   */
  public void setIncludingFilename(boolean includingFilename) {
    this.includingFilename = includingFilename;
  }

  /**
   * @return the rowNumField
   */
  public String getRowNumField() {
    return rowNumField;
  }

  /**
   * @param rowNumField the rowNumField to set
   */
  public void setRowNumField(String rowNumField) {
    this.rowNumField = rowNumField;
  }

  /**
   * @return Returns isaddresult.
   */
  public boolean isAddResultFile() {
    return isaddresult;
  }

  /**
   * @param isaddresult The isaddresult to set.
   */
  public void setAddResultFile(boolean isaddresult) {
    this.isaddresult = isaddresult;
  }

  /**
   * @return the runningInParallel
   */
  public boolean isRunningInParallel() {
    return runningInParallel;
  }

  /**
   * @param runningInParallel the runningInParallel to set
   */
  public void setRunningInParallel(boolean runningInParallel) {
    this.runningInParallel = runningInParallel;
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

  public String getBlocksID() {
    return blocksID;
  }

  public void setBlocksID(String blocksID) {
    this.blocksID = blocksID;
  }

  /**
   * Since the exported transformation that runs this will reside in a ZIP file, we can't
   * reference files relatively.
   * So what this does is turn the name of files into absolute paths OR it simply includes the
   * resource in the ZIP file.
   * For now, we'll simply turn it into an absolute path and pray that the file is on a shared
   * drive or something like that.
   * HANDLER: create options to configure this behavior
   */
  public String exportResources(VariableSpace space, Map<String, ResourceDefinition> definitions,
      ResourceNamingInterface resourceNamingInterface, Repository repository)
      throws KettleException {
    try {
      // The object that we're modifying here is a copy of the original!
      // So let's change the filename from relative to absolute by grabbing the file object...
      // In case the name of the file comes from previous steps, forget about this!
      //
      if (Const.isEmpty(filenameField)) {
        // From : ${Internal.Transformation.Filename.Directory}/../foo/bar.csv
        // To   : /home/matt/test/files/foo/bar.csv
        //
        FileObject fileObject =
            KettleVFS.getFileObject(space.environmentSubstitute(filename), space);

        // If the file doesn't exist, forget about this effort too!
        //
        if (fileObject.exists()) {
          // Convert to an absolute path...
          //
          filename = resourceNamingInterface.nameResource(fileObject, space, true);

          return filename;
        }
      }
      return null;
    } catch (Exception e) {
      throw new KettleException(e); //$NON-NLS-1$
    }
  }

  public boolean supportsErrorHandling() {
    return true;
  }

  public StepMetaInjectionInterface getStepMetaInjectionInterface() {

    return this;
  }

  public void injectStepMetadataEntries(List<StepInjectionMetaEntry> metadata) {
    for (StepInjectionMetaEntry entry : metadata) {
      KettleAttributeInterface attr = findAttribute(entry.getKey());

      // Set top level attributes...
      //
      String attributeKey = attr.getKey();
      if (entry.getValueType() != ValueMetaInterface.TYPE_NONE) {
        if ("FILENAME".equals(attributeKey)) {
          filename = (String) entry.getValue();
        } else if ("FILENAME_FIELD".equals(attributeKey)) {
          filenameField = (String) entry.getValue();
        } else if ("ROW_NUM_FIELD".equals(attributeKey)) {
          rowNumField = (String) entry.getValue();
        } else if ("HEADER_PRESENT".equals(attributeKey)) {
          headerPresent = (Boolean) entry.getValue();
        } else if ("DELIMITER".equals(attributeKey)) {
          delimiter = (String) entry.getValue();
        } else if ("ENCLOSURE".equals(attributeKey)) {
          enclosure = (String) entry.getValue();
        } else if ("ESACAPE_ENCLOSURE".equals(attributeKey)) {
          escapeEnclosure = (Boolean) entry.getValue();
        } else if ("BUFFERSIZE".equals(attributeKey)) {
          bufferSize = (String) entry.getValue();
        } else if ("LAZY_CONVERSION".equals(attributeKey)) {
          lazyConversionActive = (Boolean) entry.getValue();
        } else if ("PARALLEL".equals(attributeKey)) {
          runningInParallel = (Boolean) entry.getValue();
        } else if ("NEWLINE_POSSIBLE".equals(attributeKey)) {
          newlinePossibleInFields = (Boolean) entry.getValue();
        } else if ("ADD_FILENAME_RESULT".equals(attributeKey)) {
          isaddresult = (Boolean) entry.getValue();
        } else if ("ENCODING".equals(attributeKey)) {
          encoding = (String) entry.getValue();
        } else if ("blocksID".equals(attributeKey)) {
          blocksID = (String) entry.getValue();
        } else if ("partitionID".equals(attributeKey)) {
          partitionID = (String) entry.getValue();
        } else if ("escapeCharacter".equals(attributeKey)) {
          escapeCharacter = (String) entry.getValue();
        } else if ("quoteCharacter".equals(attributeKey)) {
          quoteCharacter = (String) entry.getValue();
        } else if ("commentCharacter".equals(attributeKey)) {
          commentCharacter = (String) entry.getValue();
        } else {
          throw new RuntimeException(
              "Unhandled metadata injection of attribute: " + attr.toString() + " - " + attr
                  .getDescription());
        }
      } else {
        if ("FIELDS".equals(attributeKey)) {
          // This entry contains a list of lists...
          // Each list contains a single CSV input field definition (one line in the dialog)
          //
          List<StepInjectionMetaEntry> inputFieldEntries = entry.getDetails();
          inputFields = new TextFileInputField[inputFieldEntries.size()];
          for (int row = 0; row < inputFieldEntries.size(); row++) {
            StepInjectionMetaEntry inputFieldEntry = inputFieldEntries.get(row);
            TextFileInputField inputField = new TextFileInputField();

            List<StepInjectionMetaEntry> fieldAttributes = inputFieldEntry.getDetails();
            for (int i = 0; i < fieldAttributes.size(); i++) {
              StepInjectionMetaEntry fieldAttribute = fieldAttributes.get(i);
              KettleAttributeInterface fieldAttr = findAttribute(fieldAttribute.getKey());

              String attributeValue = (String) fieldAttribute.getValue();
              String fieldAttributeKey = fieldAttr.getKey();
              if ("FIELD_NAME".equals(fieldAttributeKey)) {
                inputField.setName(attributeValue);
              } else if ("FIELD_TYPE".equals(fieldAttributeKey)) {
                inputField.setType(ValueMeta.getType(attributeValue));
              } else if ("FIELD_FORMAT".equals(fieldAttributeKey)) {
                inputField.setFormat(attributeValue);
              } else if ("FIELD_LENGTH".equals(fieldAttributeKey)) {
                inputField
                    .setLength(attributeValue == null ? -1 : Integer.parseInt(attributeValue));
              } else if ("FIELD_PRECISION".equals(fieldAttributeKey)) {
                inputField
                    .setPrecision(attributeValue == null ? -1 : Integer.parseInt(attributeValue));
              } else if ("FIELD_CURRENCY".equals(fieldAttributeKey)) {
                inputField.setCurrencySymbol(attributeValue);
              } else if ("FIELD_DECIMAL".equals(fieldAttributeKey)) {
                inputField.setDecimalSymbol(attributeValue);
              } else if ("FIELD_GROUP".equals(fieldAttributeKey)) {
                inputField.setGroupSymbol(attributeValue);
              } else if ("FIELD_TRIM_TYPE".equals(fieldAttributeKey)) {
                inputField.setTrimType(ValueMeta.getTrimTypeByCode(attributeValue));
              } else {
                throw new RuntimeException(
                    "Unhandled metadata injection of attribute: " + fieldAttr.toString() + " - "
                        + fieldAttr.getDescription());
              }
            }

            inputFields[row] = inputField;
          }
        }
      }
    }
  }

  /**
   * Describe the metadata attributes that can be injected into this step metadata object.
   *
   * @throws KettleException
   */
  public List<StepInjectionMetaEntry> getStepInjectionMetadataEntries() throws KettleException {
    return getStepInjectionMetadataEntries(PKG);
  }

  /**
   * @return the newlinePossibleInFields
   */
  public boolean isNewlinePossibleInFields() {
    return newlinePossibleInFields;
  }

  /**
   * @param newlinePossibleInFields the newlinePossibleInFields to set
   */
  public void setNewlinePossibleInFields(boolean newlinePossibleInFields) {
    this.newlinePossibleInFields = newlinePossibleInFields;
  }

  public void setPartitionID(String partitionID) {
    this.partitionID = partitionID;
  }

  public String getPartitionID() {
    return this.partitionID;
  }

  public String getMaxColumns() {
    return maxColumns;
  }

  public void setMaxColumns(String maxColumns) {
    this.maxColumns = maxColumns;
  }
}
