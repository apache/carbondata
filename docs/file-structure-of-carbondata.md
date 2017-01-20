#  CarbonData File Structure

CarbonData files contain groups of data called blocklets, along with all required information like schema, offsets and indices etc, in a file footer, co-located in HDFS.

The file footer can be read once to build the indices in memory, which can be utilized for optimizing the scans and processing for all subsequent queries.

Each blocklet in the file is further divided into chunks of data called data chunks. Each data chunk is organized either in columnar format or row format, and stores the data of either a single column or a set of columns. All blocklets in a file contain the same number and type of data chunks.

![CarbonData File Structure](../docs/images/carbon_data_file_structure_new.png?raw=true)

Each data chunk contains multiple groups of data called as pages. There are three types of pages.

* Data Page: Contains the encoded data of a column/group of columns.
* Row ID Page (optional): Contains the row ID mappings used when the data page is stored as an inverted index.
* RLE Page (optional): Contains additional metadata used when the data page is RLE coded.

![CarbonData File Format](../docs/images/carbon_data_format_new.png?raw=true)
