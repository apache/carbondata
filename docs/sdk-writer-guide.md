# SDK Writer Guide
In the carbon jars package, there exist a carbondata-store-sdk-x.x.x-SNAPSHOT.jar.
This SDK writer, writes carbondata file and carbonindex file at a given path.
External client can make use of this writer to convert other format data or live data to create carbondata and index files.
These SDK writer output contains just a carbondata and carbonindex files. No metadata folder will be present.

## Quick example

```scala
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
 String path ="/home/root1/Documents/ab/temp";

 Field[] fields =new Field[2]; 
 fields[0] = new Field("name", DataTypes.STRING);
 fields[1] = new Field("age", DataTypes.INT);

 Schema schema =new Schema(fields);

 CarbonWriterBuilder builder = CarbonWriter.builder()
 .withSchema(schema)
 .outputPath(path);

 CarbonWriter writer = builder.buildWriterForCSVInput();

 int rows = 5;
 for (int i = 0; i < rows; i++) {
 writer.write(new String[]{"robot" + (i % 10), String.valueOf(i)});
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


## API List
```
/**
* prepares the builder with the schema provided
* @param schema is instance of Schema
* @return updated CarbonWriterBuilder
*/
public CarbonWriterBuilder withSchema(Schema schema);
```

```
/**
* Sets the output path of the writer builder
* @param path is the absolute path where output files are written
* @return updated CarbonWriterBuilder
*/
public CarbonWriterBuilder outputPath(String path);
```

```
/**
* If set false, writes the carbondata and carbonindex files in a flat folder structure
* @param isTransactionalTable is a boolelan value if set to false then writes
*                     the carbondata and carbonindex files in a flat folder structure
* @return updated CarbonWriterBuilder
*/
public CarbonWriterBuilder isTransactionalTable(boolean isTransactionalTable);
```

```
/**
* to set the timestamp in the carbondata and carbonindex index files
* @param UUID is a timestamp to be used in the carbondata 
* and carbonindex index files
* @return updated CarbonWriterBuilder
*/
public CarbonWriterBuilder uniqueIdentifier(long UUID);
```

```
/**
* To set the carbondata file size in MB between 1MB-2048MB
* @param blockSize is size in MB between 1MB to 2048 MB
* @return updated CarbonWriterBuilder
*/
public CarbonWriterBuilder withBlockSize(int blockSize);
```

```
/**
* To set the blocklet size of carbondata file
* @param blockletSize is blocklet size in MB
* @return updated CarbonWriterBuilder
*/
public CarbonWriterBuilder withBlockletSize(int blockletSize);
```

```
/**
* sets the list of columns that needs to be in sorted order
* @param sortColumns is a string array of columns that needs to be sorted.
*                    If it is null, all dimensions are selected for sorting
*                    If it is empty array, no columns are sorted
* @return updated CarbonWriterBuilder
*/
public CarbonWriterBuilder sortBy(String[] sortColumns);
```

```
/**
* If set, creates a schema file in metadata folder.
* @param persist is a boolean value, If set, 
* creates a schema file in metadata folder
* @return updated CarbonWriterBuilder
*/
public CarbonWriterBuilder persistSchemaFile(boolean persist);
```

```
/**
* sets the taskNo for the writer. SDKs concurrently running
* will set taskNo in order to avoid conflits in file write.
* @param taskNo is the TaskNo user wants to specify. Mostly it system time.
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
* @return updated CarbonWriterBuilder
*/
public CarbonWriterBuilder withLoadOptions(Map<String, String> options);
```