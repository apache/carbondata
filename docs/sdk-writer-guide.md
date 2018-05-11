# SDK Writer Guide
In the carbon jars package, there exist a carbondata-store-sdk-x.x.x-SNAPSHOT.jar.
This SDK writer, writes carbondata file and carbonindex file at a given path.
External client can make use of this writer to convert other format data or live data to create carbondata and index files.
These SDK writer output contains just a carbondata and carbonindex files. No metadata folder will be present.

## Quick example

### Example with csv format 

```java
 import java.io.IOException;
 
 import org.apache.carbondata.common.exceptions.sql.InvalidLoadOptionException;
 import org.apache.carbondata.core.metadata.datatype.DataTypes;
 import org.apache.carbondata.sdk.file.CarbonWriter;
 import org.apache.carbondata.sdk.file.CarbonWriterBuilder;
 import org.apache.carbondata.sdk.file.Field;
 import org.apache.carbondata.sdk.file.Schema;
 
 public class TestSdk {
 
   public static void main(String[] args) throws IOException, InvalidLoadOptionException {
     testSdkWriter();
   }
 
   public static void testSdkWriter() throws IOException, InvalidLoadOptionException {
     String path = "/home/root1/Documents/ab/temp";
 
     Field[] fields = new Field[2];
     fields[0] = new Field("name", DataTypes.STRING);
     fields[1] = new Field("age", DataTypes.INT);
 
     Schema schema = new Schema(fields);
 
     CarbonWriterBuilder builder = CarbonWriter.builder().withSchema(schema).outputPath(path);
 
     CarbonWriter writer = builder.buildWriterForCSVInput();
 
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

    // prepare carbon schema from avro schema 
    org.apache.carbondata.sdk.file.Schema carbonSchema =
            AvroCarbonWriter.getCarbonSchemaFromAvroSchema(avroSchema);

    try {
      CarbonWriter writer = CarbonWriter.builder()
          .withSchema(carbonSchema)
          .outputPath(path)
          .buildWriterForAvroInput();

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


## API List

### Class org.apache.carbondata.sdk.file.CarbonWriterBuilder
```
/**
* prepares the builder with the schema provided
* @param schema is instance of Schema
*        This method must be called when building CarbonWriterBuilder
* @return updated CarbonWriterBuilder
*/
public CarbonWriterBuilder withSchema(Schema schema);
```

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
public CarbonWriterBuilder taskNo(String taskNo);
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
* @return CSVCarbonWriter
* @throws IOException
* @throws InvalidLoadOptionException
*/
public CarbonWriter buildWriterForCSVInput() throws IOException, InvalidLoadOptionException;
```

```  
/**
* Build a {@link CarbonWriter}, which accepts Avro format object
* @return AvroCarbonWriter 
* @throws IOException
* @throws InvalidLoadOptionException
*/
public CarbonWriter buildWriterForAvroInput() throws IOException, InvalidLoadOptionException;
```

### Class org.apache.carbondata.sdk.file.CarbonWriter
```
/**
* Write an object to the file, the format of the object depends on the implementation
* If AvroCarbonWriter, object is of type org.apache.avro.generic.GenericData.Record 
* If CSVCarbonWriter, object is of type String[]
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