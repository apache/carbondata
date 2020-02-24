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

CarbonData provides SDK to facilitate

1. [Writing carbondata files from other application which does not use Spark](#sdk-writer)
2. [Reading carbondata files from other application which does not use Spark](#sdk-reader)

# SDK Writer

In the carbon jars package, there exist a carbondata-sdk-x.x.x-SNAPSHOT.jar, including SDK writer and reader. 
If user want to use SDK, except carbondata-sdk-x.x.x-SNAPSHOT.jar, 
it needs carbondata-core-x.x.x-SNAPSHOT.jar, carbondata-common-x.x.x-SNAPSHOT.jar, 
carbondata-format-x.x.x-SNAPSHOT.jar, carbondata-hadoop-x.x.x-SNAPSHOT.jar and carbondata-processing-x.x.x-SNAPSHOT.jar.
What's more, user also can use carbondata-sdk.jar directly.

This SDK writer, writes carbondata file and carbonindex file at a given path.
External client can make use of this writer to convert other format data or live data to create carbondata and index files.
These SDK writer output contains just carbondata and carbonindex files. No metadata folder will be present.

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

    CarbonWriterBuilder builder = CarbonWriter.builder().outputPath(path).withCsvInput(schema).writtenBy("SDK");

    CarbonWriter writer = builder.build();

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
          .withAvroInput(new org.apache.avro.Schema.Parser().parse(avroSchema)).writtenBy("SDK").build();

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

    CarbonWriterBuilder builder = CarbonWriter.builder().outputPath(path).withJsonInput(CarbonSchema).writtenBy("SDK");

    // initialize json writer with carbon schema
    CarbonWriter writer = builder.build();
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
Each of SQL data types and Avro Data Types are mapped into data types of SDK. Following are the mapping:

| SQL DataTypes | Avro DataTypes | Mapped SDK DataTypes |
|---------------|----------------|----------------------|
| BOOLEAN | BOOLEAN | DataTypes.BOOLEAN |
| SMALLINT |  -  | DataTypes.SHORT |
| INTEGER | INTEGER | DataTypes.INT |
| BIGINT | LONG | DataTypes.LONG |
| DOUBLE | DOUBLE | DataTypes.DOUBLE |
| VARCHAR |  -  | DataTypes.STRING |
| BINARY |  -  | DataTypes.BINARY |
| FLOAT | FLOAT | DataTypes.FLOAT |
| BYTE |  -  | DataTypes.BYTE |
| DATE | DATE | DataTypes.DATE |
| TIMESTAMP |  -  | DataTypes.TIMESTAMP |
| STRING | STRING | DataTypes.STRING |
| DECIMAL | DECIMAL | DataTypes.createDecimalType(precision, scale) |
| ARRAY | ARRAY | DataTypes.createArrayType(elementType) |
| STRUCT | RECORD | DataTypes.createStructType(fields) |
|  -  | ENUM | DataTypes.STRING |
|  -  | UNION | DataTypes.createStructType(types) |
|  -  | MAP | DataTypes.createMapType(keyType, valueType) |
|  -  | TimeMillis | DataTypes.INT |
|  -  | TimeMicros | DataTypes.LONG |
|  -  | TimestampMillis | DataTypes.TIMESTAMP |
|  -  | TimestampMicros | DataTypes.TIMESTAMP |

**NOTE:**
 1. Carbon Supports below logical types of AVRO.
 a. Date
    The date logical type represents a date within the calendar, with no reference to a particular time zone or time of day.
    A date logical type annotates an Avro int, where the int stores the number of days from the unix epoch, 1 January 1970 (ISO calendar). 
 b. Timestamp (millisecond precision)
    The timestamp-millis logical type represents an instant on the global timeline, independent of a particular time zone or calendar, with a precision of one millisecond.
    A timestamp-millis logical type annotates an Avro long, where the long stores the number of milliseconds from the unix epoch, 1 January 1970 00:00:00.000 UTC.
 c. Timestamp (microsecond precision)
    The timestamp-micros logical type represents an instant on the global timeline, independent of a particular time zone or calendar, with a precision of one microsecond.
    A timestamp-micros logical type annotates an Avro long, where the long stores the number of microseconds from the unix epoch, 1 January 1970 00:00:00.000000 UTC.
 d. Decimal
    The decimal logical type represents an arbitrary-precision signed decimal number of the form <em>unscaled &#215; 10<sup>-scale</sup></em>.
    A decimal logical type annotates Avro bytes or fixed types. The byte array must contain the two's-complement representation of the unscaled integer value in big-endian byte order. The scale is fixed, and is specified using an attribute.
 e. Time (millisecond precision)
    The time-millis logical type represents a time of day, with no reference to a particular calendar, time zone or date, with a precision of one millisecond.
    A time-millis logical type annotates an Avro int, where the int stores the number of milliseconds after midnight, 00:00:00.000.
 f. Time (microsecond precision)
    The time-micros logical type represents a time of day, with no reference to a particular calendar, time zone or date, with a precision of one microsecond.
    A time-micros logical type annotates an Avro long, where the long stores the number of microseconds after midnight, 00:00:00.000000.

    
    Currently the values of logical types are not validated by carbon. 
    Expect that avro record passed by the user is already validated by avro record generator tools.    
 2. If the string data is more than 32K in length, use withTableProperties() with "long_string_columns" property
    or directly use DataTypes.VARCHAR if it is carbon schema.
 3. Avro Bytes, Fixed and Duration data types are not yet supported.
## Run SQL on files directly
Instead of creating table and query it, you can also query that file directly with SQL.

### Example
```
SELECT * FROM carbonfile.`$Path`
```
Find example code at [DirectSQLExample](https://github.com/apache/carbondata/blob/master/examples/spark/src/main/scala/org/apache/carbondata/examples/DirectSQLExample.scala) in the CarbonData repo.
## API List

### Class org.apache.carbondata.sdk.file.CarbonWriterBuilder
```
/**
 * Sets the output path of the writer builder
 *
 * @param path is the absolute path where output files are written
 *             This method must be called when building CarbonWriterBuilder
 * @return updated CarbonWriterBuilder
 */
public CarbonWriterBuilder outputPath(String path);
```

```
/**
 * To set the timestamp in the carbondata and carbonindex index files
 *
 * @param UUID is a timestamp to be used in the carbondata and carbonindex index files.
 *             By default set to zero.
 * @return updated CarbonWriterBuilder
 */
public CarbonWriterBuilder uniqueIdentifier(long UUID);
```

```
/**
 * To set the carbondata file size in MB between 1MB-2048MB
 *
 * @param blockSize is size in MB between 1MB to 2048 MB
 *                  default value is 1024 MB
 * @return updated CarbonWriterBuilder
 */
public CarbonWriterBuilder withBlockSize(int blockSize);
```

```
/**
 * To set the blocklet size of carbondata file
 *
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
 * Sets the list of columns that needs to be in sorted order
 *
 * @param sortColumns is a string array of columns that needs to be sorted.
 *                    If it is null or by default all dimensions are selected for sorting
 *                    If it is empty array, no columns are sorted
 * @return updated CarbonWriterBuilder
 */
public CarbonWriterBuilder sortBy(String[] sortColumns);
```

```
/**
 * Sets the taskNo for the writer. SDKs concurrently running
 * will set taskNo in order to avoid conflicts in file's name during write.
 *
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
 * To support the table properties for sdk writer
 *
 * @param options key,value pair of create table properties.
 * supported keys values are
 * a. table_blocksize -- [1-2048] values in MB. Default value is 1024
 * b. table_blocklet_size -- values in MB. Default value is 64 MB
 * c. local_dictionary_threshold -- positive value, default is 10000
 * d. local_dictionary_enable -- true / false. Default is false
 * e. sort_columns -- comma separated column. "c1,c2". Default no columns are sorted.
 * j. sort_scope -- "local_sort", "no_sort". default value is "no_sort"
 * k. long_string_columns -- comma separated string columns which are more than 32k length. 
 *                           default value is null.
 * l. inverted_index -- comma separated string columns for which inverted index needs to be
 *                      generated
 * m. table_page_size_inmb -- [1-1755] MB. 
 *
 * @return updated CarbonWriterBuilder
 */
public CarbonWriterBuilder withTableProperties(Map<String, String> options);
```

```
/**
 * To make sdk writer thread safe.
 *
 * @param numOfThreads should number of threads in which writer is called in multi-thread scenario
 *                     default sdk writer is not thread safe.
 *                     can use one writer instance in one thread only.
 * @return updated CarbonWriterBuilder
 */
public CarbonWriterBuilder withThreadSafe(short numOfThreads);
```

```
/**
 * To support hadoop configuration
 *
 * @param conf hadoop configuration support, can set s3a AK,SK,end point and other conf with this
 * @return updated CarbonWriterBuilder
 */
public CarbonWriterBuilder withHadoopConf(Configuration conf)
```

```
/**
 * Updates the hadoop configuration with the given key value
 *
 * @param key   key word
 * @param value value
 * @return this object
 */
public CarbonWriterBuilder withHadoopConf(String key, String value);
```

```
/**
 * To build a {@link CarbonWriter}, which accepts row in CSV format
 *
 * @param schema carbon Schema object {org.apache.carbondata.sdk.file.Schema}
 * @return CarbonWriterBuilder
 */
public CarbonWriterBuilder withCsvInput(Schema schema);
```

```
/**
 * To build a {@link CarbonWriter}, which accepts Avro object
 *
 * @param avroSchema avro Schema object {org.apache.avro.Schema}
 * @return CarbonWriterBuilder
 */
public CarbonWriterBuilder withAvroInput(org.apache.avro.Schema avroSchema);
```

```
/**
 * To build a {@link CarbonWriter}, which accepts Json object
 *
 * @param carbonSchema carbon Schema object
 * @return CarbonWriterBuilder
 */
public CarbonWriterBuilder withJsonInput(Schema carbonSchema);
```

```
/**
 * To support writing the ApplicationName which is writing the carbondata file
 * This is a mandatory API to call, else the build() call will fail with error.
 * @param application name which is writing the carbondata files
 * @return CarbonWriterBuilder
 */
public CarbonWriterBuilder writtenBy(String appName) {
```

```
/**
 * Sets the list of columns for which inverted index needs to generated
 *
 * @param invertedIndexColumns is a string array of columns for which inverted index needs to
 * generated.
 * If it is null or an empty array, inverted index will be generated for none of the columns
 * @return updated CarbonWriterBuilder
 */
public CarbonWriterBuilder invertedIndexFor(String[] invertedIndexColumns);
```

```
/**
 * Build a {@link CarbonWriter}
 * This writer is not thread safe,
 * use withThreadSafe() configuration in multi thread environment
 * 
 * @return CarbonWriter {AvroCarbonWriter/CSVCarbonWriter/JsonCarbonWriter based on Input Type }
 * @throws IOException
 * @throws InvalidLoadOptionException
 */
public CarbonWriter build() throws IOException, InvalidLoadOptionException;
```

```
/**
 * Configure Row Record Reader for reading.
 *
 */
public CarbonReaderBuilder withRowRecordReader()
```

### Class org.apache.carbondata.sdk.file.CarbonWriter

```
/**
 * Create a {@link CarbonWriterBuilder} to build a {@link CarbonWriter}
 */
public static CarbonWriterBuilder builder() {
    return new CarbonWriterBuilder();
}
```

```
/**
 * Write an object to the file, the format of the object depends on the implementation
 * If AvroCarbonWriter, object is of type org.apache.avro.generic.GenericData.Record, 
 *                      which is one row of data.
 * If CSVCarbonWriter, object is of type String[], which is one row of data
 * If JsonCarbonWriter, object is of type String, which is one row of json
 *
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

### Class org.apache.carbondata.sdk.file.Field
```
/**
 * Field Constructor
 *
 * @param name name of the field
 * @param type datatype of field, specified in strings.
 */
public Field(String name, String type);
```

```
/**
 * Field constructor
 *
 * @param name name of the field
 * @param type datatype of the field of class DataType
 */
public Field(String name, DataType type);  
```

### Class org.apache.carbondata.sdk.file.Schema

```
/**
 * Construct a schema with fields
 *
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
 * Converts avro schema to carbon schema, required by carbonWriter
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

Find example code at [CarbonReaderExample](https://github.com/apache/carbondata/blob/master/examples/spark/src/main/java/org/apache/carbondata/examples/sdk/CarbonReaderExample.java) in the CarbonData repo.

SDK reader also supports reading carbondata files and filling it to apache arrow vectors.
Find example code at [ArrowCarbonReaderTest](https://github.com/apache/carbondata/blob/master/sdk/sdk/src/test/java/org/apache/carbondata/sdk/file/ArrowCarbonReaderTest.java) in the CarbonData repo.


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
 * Breaks the list of CarbonRecordReader in CarbonReader into multiple
 * CarbonReader objects, each iterating through some 'carbondata' files
 * and return that list of CarbonReader objects
 *
 * If the no. of files is greater than maxSplits, then break the
 * CarbonReader into maxSplits splits, with each split iterating
 * through >= 1 file.
 *
 * If the no. of files is less than maxSplits, then return list of
 * CarbonReader with size as the no. of files, with each CarbonReader
 * iterating through exactly one file
 *
 * @param maxSplits: Int
 * @return list of CarbonReader objects
 */
public List<CarbonReader> split(int maxSplits);
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
 * Read and return next batch row objects
 */
public Object[] readNextBatchRow();
```

```
/**
 * Close reader
 */
public void close();
```

### Class org.apache.carbondata.sdk.file.ArrowCarbonReader
```
/**
 * Carbon reader will fill the arrow vector after reading the carbondata files.
 * This arrow byte[] can be used to create arrow table and used for in memory analytics
 * Note: create a reader at blocklet level, so that arrow byte[] will not exceed INT_MAX
 *
 * @param carbonSchema org.apache.carbondata.sdk.file.Schema
 * @return Serialized byte array
 * @throws Exception
 */
public byte[] readArrowBatch(Schema carbonSchema) throws Exception;
```

```
/**
 * Carbon reader will fill the arrow vector after reading the carbondata files.
 * This arrow byte[] can be used to create arrow table and used for in memory analytics
 * Note: create a reader at blocklet level, so that arrow byte[] will not exceed INT_MAX
 * User need to close the VectorSchemaRoot after usage by calling VectorSchemaRoot.close()
 *
 * @param carbonSchema org.apache.carbondata.sdk.file.Schema 
 * @return Arrow VectorSchemaRoot
 * @throws Exception
 */
public VectorSchemaRoot readArrowVectors(Schema carbonSchema) throws Exception;
```

```
/**
 * Carbon reader will fill the arrow vector after reading carbondata files.
 * Here unsafe memory address will be returned instead of byte[],
 * so that this address can be sent across java to python or c modules and
 * can directly read the content from this unsafe memory
 * Note:Create a carbon reader at blocklet level using CarbonReader.buildWithSplits(split) method,
 * so that arrow byte[] will not exceed INT_MAX.
 *
 * @param carbonSchema org.apache.carbondata.sdk.file.Schema
 * @return address of the unsafe memory where arrow buffer is stored
 * @throws Exception
 */
public long readArrowBatchAddress(Schema carbonSchema) throws Exception;
```

```
/**
 * Free the unsafe memory allocated , if unsafe arrow batch is used.
 *
 * @param address address of the unsafe memory where arrow bufferer is stored
 */
public void freeArrowBatchMemory(long address)
```

### Class org.apache.carbondata.sdk.file.arrow.ArrowConverter
```
/**
 * To get the arrow vectors directly after filling from carbondata
 *
 * @return Arrow VectorSchemaRoot. which contains array of arrow vectors.
 */
public VectorSchemaRoot getArrowVectors() throws IOException;
```

```
/**
 * Utility API to convert back the arrow byte[] to arrow ArrowRecordBatch.
 * User need to close the ArrowRecordBatch after usage by calling ArrowRecordBatch.close()
 *
 * @param batchBytes input byte array
 * @param bufferAllocator arrow buffer allocator
 * @return ArrowRecordBatch
 * @throws IOException
 */
public static ArrowRecordBatch byteArrayToArrowBatch(byte[] batchBytes, BufferAllocator bufferAllocator) throws IOException;
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
 * Configure the filter expression for carbon reader
 *
 * @param filterExpression filter expression
 * @return CarbonReaderBuilder object
 */
public CarbonReaderBuilder filter(Expression filterExpression);
```

```
/**
 * Sets the batch size of records to read
 *
 * @param batch batch size
 * @return updated CarbonReaderBuilder
 */
public CarbonReaderBuilder withBatch(int batch);
```

```
/**
 * To support hadoop configuration
 *
 * @param conf hadoop configuration support, can set s3a AK,SK,end point and other conf with this
 * @return updated CarbonReaderBuilder
 */
public CarbonReaderBuilder withHadoopConf(Configuration conf);
```

```
/**
 * Updates the hadoop configuration with the given key value
 *
 * @param key   key word
 * @param value value
 * @return this object
 */
public CarbonReaderBuilder withHadoopConf(String key, String value);
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
@Deprecated
public static Schema readSchemaInSchemaFile(String schemaFilePath);
```

```
/**
 * Read carbondata file and return the schema
 *
 * @param dataFilePath complete path including carbondata file name
 * @return Schema object
 */
@Deprecated
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
@Deprecated
public static Schema readSchemaInIndexFile(String indexFilePath);
```

```
/**
 * Read schema from path,
 * path can be folder path,carbonindex file path, and carbondata file path
 * and will not check all files schema
 *
 * @param path file/folder path
 * @return schema
 * @throws IOException
 */
public static Schema readSchema(String path);
```

```
/**
 * Read schema from path,
 * path can be folder path,carbonindex file path, and carbondata file path
 * and user can decide whether check all files schema
 *
 * @param path             file/folder path
 * @param validateSchema whether check all files schema
 * @return schema
 * @throws IOException
 */
public static Schema readSchema(String path, boolean validateSchema);
```

```
/**
 * Read schema from path,
 * path can be folder path, carbonindex file path, and carbondata file path
 * and will not check all files schema
 *
 * @param path file/folder path
 * @param conf hadoop configuration support, can set s3a AK,SK,end point and other conf with this
 * @return schema
 * @throws IOException
 */
public static Schema readSchema(String path, Configuration conf);
```

```
/**
 * Read schema from path,
 * path can be folder path, carbonindex file path, and carbondata file path
 * and user can decide whether check all files schema
 *
 * @param path           file/folder path
 * @param validateSchema whether check all files schema
 * @param conf           hadoop configuration support, can set s3a AK,SK,
 *                       end point and other conf with this
 * @return schema
 * @throws IOException
 */
public static Schema readSchema(String path, boolean validateSchema, Configuration conf);
```

```
/**
 * This method return the version details in formatted string by reading from carbondata file
 * If application name is SDK_1.0.0 and this has written the carbondata file in carbondata 1.6 project version,
 * then this API returns the String "SDK_1.0.0 in version: 1.6.0-SNAPSHOT"
 *
 * @param dataFilePath complete path including carbondata file name
 * @return string with information of who has written this file in which carbondata project version
 * @throws IOException
 */
public static String getVersionDetails(String dataFilePath);
```

### Class org.apache.carbondata.sdk.file.Schema
```
/**
 * Construct a schema with fields
 *
 * @param fields
 */
public Schema(Field[] fields);
```

```
/**
 * Construct a schema with List<ColumnSchema>
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
 *
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

Find S3 example code at [SDKS3Example](https://github.com/apache/carbondata/blob/master/examples/spark/src/main/java/org/apache/carbondata/examples/sdk/SDKS3Example.java) in the CarbonData repo.


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
Reference : [list of carbon properties](./configuration-parameters.md)

