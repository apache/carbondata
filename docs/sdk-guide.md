<!--
    Licensed to the Apache Software Foundation (ASF) under one or more 
    contributor license agreements.  See the NOTICE file distributed with
    this work for additional information regarding copyright ownership. 
    The ASF licenses this file to you under the Apache License, Version 2.0
    (the "License"); you may not use this file except in compliance with 
    the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software 
    distributed under the License is distributed on an "AS IS" BASIS, 
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and 
    limitations under the License.
-->

# SDK Guide
In the carbon jars package, there exist a carbondata-store-sdk-x.x.x-SNAPSHOT.jar, including SDK writer and reader.
# SDK Writer
This SDK writer, writes carbondata file and carbonindex file at a given path.
External client can make use of this writer to convert other format data or live data to create carbondata and index files.
These SDK writer output contains just a carbondata and carbonindex files. No metadata folder will be present.

## Quick example

### Example with csv format 

```java
 import java.io.IOException;
 
 import org.apache.carbondata.common.exceptions.sql.InvalidLoadOptionException;
 import org.apache.carbondata.core.metadata.datatype.DataTypes;
 import org.apache.carbondata.core.util.CarbonProperties;
 import org.apache.carbondata.sdk.file.CarbonWriter;
 import org.apache.carbondata.sdk.file.CarbonWriterBuilder;
 import org.apache.carbondata.sdk.file.Field;
 import org.apache.carbondata.sdk.file.Schema;
 
 public class TestSdk {

   // pass true or false while executing the main to use offheap memory or not
   public static void main(String[] args) throws IOException, InvalidLoadOptionException {
     if (args.length > 0 && args[0] != null) {
       testSdkWriter(args[0]);
     } else {
       testSdkWriter("true");
     }
   }
 
   public static void testSdkWriter(String enableOffheap) throws IOException, InvalidLoadOptionException {
     String path = "./target/testCSVSdkWriter";
 
     Field[] fields = new Field[2];
     fields[0] = new Field("name", DataTypes.STRING);
     fields[1] = new Field("age", DataTypes.INT);
 
     Schema schema = new Schema(fields);

     CarbonProperties.getInstance().addProperty("enable.offheap.sort", enableOffheap);
 
     CarbonWriterBuilder builder = CarbonWriter.builder().outputPath(path);
 
     CarbonWriter writer = builder.buildWriterForCSVInput(schema);
 
     int rows = 5;
     for (int i = 0; i < rows; i++) {
       writer.write(new String[] { "robot" + (i % 10), String.valueOf(i) });
     }
     writer.close();
   }
 }
```

### Example with Avro format
```java
import java.io.IOException;

import org.apache.carbondata.common.exceptions.sql.InvalidLoadOptionException;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.sdk.file.AvroCarbonWriter;
import org.apache.carbondata.sdk.file.CarbonWriter;
import org.apache.carbondata.sdk.file.Field;

import org.apache.avro.generic.GenericData;
import org.apache.commons.lang.CharEncoding;

import tech.allegro.schema.json2avro.converter.JsonAvroConverter;

public class TestSdkAvro {

  public static void main(String[] args) throws IOException, InvalidLoadOptionException {
    testSdkWriter();
  }


  public static void testSdkWriter() throws IOException, InvalidLoadOptionException {
    String path = "./AvroCarbonWriterSuiteWriteFiles";
    // Avro schema
    String avroSchema =
        "{" +
            "   \"type\" : \"record\"," +
            "   \"name\" : \"Acme\"," +
            "   \"fields\" : ["
            + "{ \"name\" : \"fname\", \"type\" : \"string\" },"
            + "{ \"name\" : \"age\", \"type\" : \"int\" }]" +
            "}";

    String json = "{\"fname\":\"bob\", \"age\":10}";

    // conversion to GenericData.Record
    JsonAvroConverter converter = new JsonAvroConverter();
    GenericData.Record record = converter.convertToGenericDataRecord(
        json.getBytes(CharEncoding.UTF_8), new org.apache.avro.Schema.Parser().parse(avroSchema));

    try {
      CarbonWriter writer = CarbonWriter.builder()
          .outputPath(path)
          .buildWriterForAvroInput(new org.apache.avro.Schema.Parser().parse(avroSchema));

      for (int i = 0; i < 100; i++) {
        writer.write(record);
      }
      writer.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
```

### Example with Json format
```java
import java.io.IOException;
 
import org.apache.carbondata.common.exceptions.sql.InvalidLoadOptionException;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.sdk.file.CarbonWriter;
import org.apache.carbondata.sdk.file.CarbonWriterBuilder;
import org.apache.carbondata.sdk.file.Field;
import org.apache.carbondata.sdk.file.Schema;
 
public class TestSdkJson {

   public static void main(String[] args) throws InvalidLoadOptionException {
       testJsonSdkWriter();
   }
   
   public static void testJsonSdkWriter() throws InvalidLoadOptionException {
    String path = "./target/testJsonSdkWriter";

    Field[] fields = new Field[2];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);

    Schema CarbonSchema = new Schema(fields);

    CarbonWriterBuilder builder = CarbonWriter.builder().outputPath(path);

    // initialize json writer with carbon schema
    CarbonWriter writer = builder.buildWriterForJsonInput(CarbonSchema);
    // one row of json Data as String
    String  JsonRow = "{\"name\":\"abcd\", \"age\":10}";

    int rows = 5;
    for (int i = 0; i < rows; i++) {
      writer.write(JsonRow);
    }
    writer.close();
  }
} 
```

## Datatypes Mapping
Each of SQL data types are mapped into data types of SDK. Following are the mapping:

| SQL DataTypes | Mapped SDK DataTypes |
|---------------|----------------------|
| BOOLEAN | DataTypes.BOOLEAN |
| SMALLINT | DataTypes.SHORT |
| INTEGER | DataTypes.INT |
| BIGINT | DataTypes.LONG |
| DOUBLE | DataTypes.DOUBLE |
| VARCHAR | DataTypes.STRING |
| DATE | DataTypes.DATE |
| TIMESTAMP | DataTypes.TIMESTAMP |
| STRING | DataTypes.STRING |
| DECIMAL | DataTypes.createDecimalType(precision, scale) |

**NOTE:**
 Carbon Supports below logical types of AVRO.
 a. Date
    The date logical type represents a date within the calendar, with no reference to a particular time zone or time of day.
    A date logical type annotates an Avro int, where the int stores the number of days from the unix epoch, 1 January 1970 (ISO calendar). 
 b. Timestamp (millisecond precision)
    The timestamp-millis logical type represents an instant on the global timeline, independent of a particular time zone or calendar, with a precision of one millisecond.
    A timestamp-millis logical type annotates an Avro long, where the long stores the number of milliseconds from the unix epoch, 1 January 1970 00:00:00.000 UTC.
 c. Timestamp (microsecond precision)
    The timestamp-micros logical type represents an instant on the global timeline, independent of a particular time zone or calendar, with a precision of one microsecond.
    A timestamp-micros logical type annotates an Avro long, where the long stores the number of microseconds from the unix epoch, 1 January 1970 00:00:00.000000 UTC.
    
    Currently the values of logical types are not validated by carbon. 
    Expect that avro record passed by the user is already validated by avro record generator tools.   

## Run SQL on files directly
Instead of creating table and query it, you can also query that file directly with SQL.

### Example
```
SELECT * FROM carbonfile.`$Path`
```
Find example code at [DirectSQLExample](https://github.com/apache/carbondata/blob/master/examples/spark2/src/main/scala/org/apache/carbondata/examples/DirectSQLExample.scala) in the CarbonData repo.
## API List

### Class org.apache.carbondata.sdk.file.CarbonWriterBuilder
```
/**
* Sets the output path of the writer builder
* @param path is the absolute path where output files are written
*             This method must be called when building CarbonWriterBuilder
* @return updated CarbonWriterBuilder
*/
public CarbonWriterBuilder outputPath(String path);
```

```
/**
* If set false, writes the carbondata and carbonindex files in a flat folder structure
* @param isTransactionalTable is a boolelan value
*             if set to false, then writes the carbondata and carbonindex files
*                                                            in a flat folder structure.
*             if set to true, then writes the carbondata and carbonindex files
*                                                            in segment folder structure..
*             By default set to false.
* @return updated CarbonWriterBuilder
*/
public CarbonWriterBuilder isTransactionalTable(boolean isTransactionalTable);
```

```
/**
* to set the timestamp in the carbondata and carbonindex index files
* @param UUID is a timestamp to be used in the carbondata and carbonindex index files.
*             By default set to zero.
* @return updated CarbonWriterBuilder
*/
public CarbonWriterBuilder uniqueIdentifier(long UUID);
```

```
/**
* To set the carbondata file size in MB between 1MB-2048MB
* @param blockSize is size in MB between 1MB to 2048 MB
*                  default value is 1024 MB
* @return updated CarbonWriterBuilder
*/
public CarbonWriterBuilder withBlockSize(int blockSize);
```

```
/**
* To set the blocklet size of carbondata file
* @param blockletSize is blocklet size in MB
*                     default value is 64 MB
* @return updated CarbonWriterBuilder
*/
public CarbonWriterBuilder withBlockletSize(int blockletSize);
```

```
/**
   * @param enableLocalDictionary enable local dictionary  , default is false
   * @return updated CarbonWriterBuilder
   */
public CarbonWriterBuilder enableLocalDictionary(boolean enableLocalDictionary);
```

```
/**
   * @param localDictionaryThreshold is localDictionaryThreshold,default is 10000
   * @return updated CarbonWriterBuilder
   */
public CarbonWriterBuilder localDictionaryThreshold(int localDictionaryThreshold) ;
```


```
/**
* sets the list of columns that needs to be in sorted order
* @param sortColumns is a string array of columns that needs to be sorted.
*                    If it is null or by default all dimensions are selected for sorting
*                    If it is empty array, no columns are sorted
* @return updated CarbonWriterBuilder
*/
public CarbonWriterBuilder sortBy(String[] sortColumns);
```

```
/**
* If set, create a schema file in metadata folder.
* @param persist is a boolean value, If set to true, creates a schema file in metadata folder.
*                By default set to false. will not create metadata folder
* @return updated CarbonWriterBuilder
*/
public CarbonWriterBuilder persistSchemaFile(boolean persist);
```

```
/**
* sets the taskNo for the writer. SDKs concurrently running
* will set taskNo in order to avoid conflicts in file's name during write.
* @param taskNo is the TaskNo user wants to specify.
*               by default it is system time in nano seconds.
* @return updated CarbonWriterBuilder
*/
public CarbonWriterBuilder taskNo(long taskNo);
```

```
/**
* To support the load options for sdk writer
* @param options key,value pair of load options.
*                supported keys values are
*                a. bad_records_logger_enable -- true (write into separate logs), false
*                b. bad_records_action -- FAIL, FORCE, IGNORE, REDIRECT
*                c. bad_record_path -- path
*                d. dateformat -- same as JAVA SimpleDateFormat
*                e. timestampformat -- same as JAVA SimpleDateFormat
*                f. complex_delimiter_level_1 -- value to Split the complexTypeData
*                g. complex_delimiter_level_2 -- value to Split the nested complexTypeData
*                h. quotechar
*                i. escapechar
*
*                Default values are as follows.
*
*                a. bad_records_logger_enable -- "false"
*                b. bad_records_action -- "FAIL"
*                c. bad_record_path -- ""
*                d. dateformat -- "" , uses from carbon.properties file
*                e. timestampformat -- "", uses from carbon.properties file
*                f. complex_delimiter_level_1 -- "$"
*                g. complex_delimiter_level_2 -- ":"
*                h. quotechar -- "\""
*                i. escapechar -- "\\"
*
* @return updated CarbonWriterBuilder
*/
public CarbonWriterBuilder withLoadOptions(Map<String, String> options);
```

```
/**
* Build a {@link CarbonWriter}, which accepts row in CSV format object
* @param schema carbon Schema object {org.apache.carbondata.sdk.file.Schema}
* @return CSVCarbonWriter
* @throws IOException
* @throws InvalidLoadOptionException
*/
public CarbonWriter buildWriterForCSVInput(org.apache.carbondata.sdk.file.Schema schema) throws IOException, InvalidLoadOptionException;
```

```  
/**
* Build a {@link CarbonWriter}, which accepts Avro format object
* @param avroSchema avro Schema object {org.apache.avro.Schema}
* @return AvroCarbonWriter 
* @throws IOException
* @throws InvalidLoadOptionException
*/
public CarbonWriter buildWriterForAvroInput(org.apache.avro.Schema schema) throws IOException, InvalidLoadOptionException;
```

```
/**
* Build a {@link CarbonWriter}, which accepts Json object
* @param carbonSchema carbon Schema object
* @return JsonCarbonWriter
* @throws IOException
* @throws InvalidLoadOptionException
*/
public JsonCarbonWriter buildWriterForJsonInput(Schema carbonSchema);
```

### Class org.apache.carbondata.sdk.file.CarbonWriter
```
/**
* Write an object to the file, the format of the object depends on the implementation
* If AvroCarbonWriter, object is of type org.apache.avro.generic.GenericData.Record, 
*                      which is one row of data.
* If CSVCarbonWriter, object is of type String[], which is one row of data
* If JsonCarbonWriter, object is of type String, which is one row of json
* Note: This API is not thread safe
* @param object
* @throws IOException
*/
public abstract void write(Object object) throws IOException;
```

```
/**
* Flush and close the writer
*/
public abstract void close() throws IOException;
```

```
/**
* Create a {@link CarbonWriterBuilder} to build a {@link CarbonWriter}
*/
public static CarbonWriterBuilder builder() {
    return new CarbonWriterBuilder();
}
```

### Class org.apache.carbondata.sdk.file.Field
```
/**
* Field Constructor
* @param name name of the field
* @param type datatype of field, specified in strings.
*/
public Field(String name, String type);
```

```
/**
* Field constructor
* @param name name of the field
* @param type datatype of the field of class DataType
*/
public Field(String name, DataType type);  
```

### Class org.apache.carbondata.sdk.file.Schema

```
/**
* construct a schema with fields
* @param fields
*/
public Schema(Field[] fields);
```

```
/**
* Create a Schema using JSON string, for example:
* [
*   {"name":"string"},
*   {"age":"int"}
* ] 
* @param json specified as string
* @return Schema
*/
public static Schema parseJson(String json);
```

### Class org.apache.carbondata.sdk.file.AvroCarbonWriter
```
/**
* converts avro schema to carbon schema, required by carbonWriter
*
* @param avroSchemaString json formatted avro schema as string
* @return carbon sdk schema
*/
public static org.apache.carbondata.sdk.file.Schema getCarbonSchemaFromAvroSchema(String avroSchemaString);
```
# SDK Reader
This SDK reader reads CarbonData file and carbonindex file at a given path.
External client can make use of this reader to read CarbonData files without CarbonSession.
## Quick example
```
    // 1. Create carbon reader
    String path = "./testWriteFiles";
    CarbonReader reader = CarbonReader
        .builder(path, "_temp")
        .projection(new String[]{"stringField", "shortField", "intField", "longField", 
                "doubleField", "boolField", "dateField", "timeField", "decimalField"})
        .build();

    // 2. Read data
    long day = 24L * 3600 * 1000;
    int i = 0;
    while (reader.hasNext()) {
        Object[] row = (Object[]) reader.readNextRow();
        System.out.println(String.format("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t",
            i, row[0], row[1], row[2], row[3], row[4], row[5],
            new Date((day * ((int) row[6]))), new Timestamp((long) row[7] / 1000), row[8]
        ));
        i++;
    }

    // 3. Close this reader
    reader.close();
```

Find example code at [CarbonReaderExample](https://github.com/apache/carbondata/blob/master/examples/spark2/src/main/java/org/apache/carbondata/examples/sdk/CarbonReaderExample.java) in the CarbonData repo.

## API List

### Class org.apache.carbondata.sdk.file.CarbonReader
```
   /**
    * Return a new {@link CarbonReaderBuilder} instance
    *
    * @param tablePath table store path
    * @param tableName table name
    * @return CarbonReaderBuilder object
    */
  public static CarbonReaderBuilder builder(String tablePath, String tableName);
```

```
  /**
   * Return a new CarbonReaderBuilder instance
   * Default value of table name is table + tablePath + time
   *
   * @param tablePath table path
   * @return CarbonReaderBuilder object
   */
  public static CarbonReaderBuilder builder(String tablePath);
```

```
  /**
   * Return true if has next row
   */
  public boolean hasNext();
```

```
  /**
   * Read and return next row object
   */
  public T readNextRow();
```

```
  /**
   * Close reader
   */
  public void close();
```

### Class org.apache.carbondata.sdk.file.CarbonReaderBuilder
```
  /**
   * Construct a CarbonReaderBuilder with table path and table name
   *
   * @param tablePath table path
   * @param tableName table name
   */
  CarbonReaderBuilder(String tablePath, String tableName);
```

```
  /**
   * Configure the projection column names of carbon reader
   *
   * @param projectionColumnNames projection column names
   * @return CarbonReaderBuilder object
   */
  public CarbonReaderBuilder projection(String[] projectionColumnNames);
```

```
  /**
   * Configure the transactional status of table
   * If set to false, then reads the carbondata and carbonindex files from a flat folder structure.
   * If set to true, then reads the carbondata and carbonindex files from segment folder structure.
   * Default value is false
   *
   * @param isTransactionalTable whether is transactional table or not
   * @return CarbonReaderBuilder object
   */
  public CarbonReaderBuilder isTransactionalTable(boolean isTransactionalTable);
```

```
 /**
  * Configure the filter expression for carbon reader
  *
  * @param filterExpression filter expression
  * @return CarbonReaderBuilder object
  */
  public CarbonReaderBuilder filter(Expression filterExpression);
```

```
  /**
   * Set the access key for S3
   *
   * @param key   the string of access key for different S3 type,like: fs.s3a.access.key
   * @param value the value of access key
   * @return CarbonWriterBuilder
   */
  public CarbonReaderBuilder setAccessKey(String key, String value);
```

```
  /**
   * Set the access key for S3.
   *
   * @param value the value of access key
   * @return CarbonWriterBuilder object
   */
  public CarbonReaderBuilder setAccessKey(String value);
```

```
  /**
   * Set the secret key for S3
   *
   * @param key   the string of secret key for different S3 type,like: fs.s3a.secret.key
   * @param value the value of secret key
   * @return CarbonWriterBuilder object
   */
  public CarbonReaderBuilder setSecretKey(String key, String value);
```

```
  /**
   * Set the secret key for S3
   *
   * @param value the value of secret key
   * @return CarbonWriterBuilder object
   */
  public CarbonReaderBuilder setSecretKey(String value);
```

```
 /**
   * Set the endpoint for S3
   *
   * @param key   the string of endpoint for different S3 type,like: fs.s3a.endpoint
   * @param value the value of endpoint
   * @return CarbonWriterBuilder object
   */
  public CarbonReaderBuilder setEndPoint(String key, String value);
```

``` 
  /**
   * Set the endpoint for S3
   *
   * @param value the value of endpoint
   * @return CarbonWriterBuilder object
   */
  public CarbonReaderBuilder setEndPoint(String value);
```

```
 /**
   * Build CarbonReader
   *
   * @param <T>
   * @return CarbonReader
   * @throws IOException
   * @throws InterruptedException
   */
  public <T> CarbonReader<T> build();
```
### Class org.apache.carbondata.sdk.file.CarbonSchemaReader
```
  /**
   * Read schema file and return the schema
   *
   * @param schemaFilePath complete path including schema file name
   * @return schema object
   * @throws IOException
   */
  public static Schema readSchemaInSchemaFile(String schemaFilePath);
```

```
  /**
   * Read carbondata file and return the schema
   *
   * @param dataFilePath complete path including carbondata file name
   * @return Schema object
   * @throws IOException
   */
  public static Schema readSchemaInDataFile(String dataFilePath);
```

```
  /**
   * Read carbonindex file and return the schema
   *
   * @param indexFilePath complete path including index file name
   * @return schema object
   * @throws IOException
   */
  public static Schema readSchemaInIndexFile(String indexFilePath);
```

### Class org.apache.carbondata.sdk.file.Schema
```
  /**
   * construct a schema with fields
   * @param fields
   */
  public Schema(Field[] fields);
```

```
  /**
   * construct a schema with List<ColumnSchema>
   *
   * @param columnSchemaList column schema list
   */
  public Schema(List<ColumnSchema> columnSchemaList);
```

```
  /**
   * Create a Schema using JSON string, for example:
   * [
   *   {"name":"string"},
   *   {"age":"int"}
   * ]
   * @param json specified as string
   * @return Schema
   */
  public static Schema parseJson(String json);
```

```
  /**
   * Sort the schema order as original order
   *
   * @return Schema object
   */
  public Schema asOriginOrder();
```

### Class org.apache.carbondata.sdk.file.Field
```
  /**
   * Field Constructor
   * @param name name of the field
   * @param type datatype of field, specified in strings.
   */
  public Field(String name, String type);
```

```
  /**
   * Construct Field from ColumnSchema
   *
   * @param columnSchema ColumnSchema, Store the information about the column meta data
   */
  public Field(ColumnSchema columnSchema);
```

Find S3 example code at [SDKS3Example](https://github.com/apache/carbondata/blob/master/examples/spark2/src/main/java/org/apache/carbondata/examples/sdk/SDKS3Example.java) in the CarbonData repo.


# Common API List for CarbonReader and CarbonWriter

### Class org.apache.carbondata.core.util.CarbonProperties

```
/**
* This method will be responsible to get the instance of CarbonProperties class
*
* @return carbon properties instance
*/
public static CarbonProperties getInstance();
```

```
/**
* This method will be used to add a new property
*
* @param key is a property name to set for carbon.
* @param value is valid parameter corresponding to property.
* @return CarbonProperties object
*/
public CarbonProperties addProperty(String key, String value);
```

```
/**
* This method will be used to get the property value. If property is not
* present, then it will return the default value.
*
* @param key is a property name to get user specified value.
* @return properties value for corresponding key. If not set, then returns null.
*/
public String getProperty(String key);
```

```
/**
* This method will be used to get the property value. If property is not
* present, then it will return the default value.
*
* @param key is a property name to get user specified value..
* @param defaultValue used to be returned by function if corrosponding key not set.
* @return properties value for corresponding key. If not set, then returns specified defaultValue.
*/
public String getProperty(String key, String defaultValue);
```
Reference : [list of carbon properties](http://carbondata.apache.org/configuration-parameters.html)
