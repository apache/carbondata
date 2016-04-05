package org.carbondata.core.carbon.metadata.schema.table;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonMeasure;
import org.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema;

/**
 * Mapping class for Carbon actual table
 */
public class CarbonTable implements Serializable {

    /**
     * serialization id
     */
    private static final long serialVersionUID = 8696507171227156445L;

    /**
     * database name
     */
    private String databaseName;

    /**
     * TableName, Dimensions list
     */
    private Map<String, List<CarbonDimension>> tableDimensionsMap;

    /**
     * table measures list.
     */
    private Map<String, List<CarbonMeasure>> tableMeasuresMap;

    /**
     * TableName, Measures list.
     */
    private String factTableName;

    /**
     * tableUniqueName
     */
    private String tableUniqueName;

    /**
     * metadata file path (check if it is really required )
     */
    private String metaDataFilepath;

    /**
     * last updated time
     */
    private long tableLastUpdatedTime;

    public CarbonTable() {
        this.tableDimensionsMap = new HashMap<String, List<CarbonDimension>>();
        this.tableMeasuresMap = new HashMap<String, List<CarbonMeasure>>();
    }

    public void loadCarbonTable(TableInfo tableInfo) {
        this.tableLastUpdatedTime = tableInfo.getLastUpdatedTime();
        this.databaseName = tableInfo.getDatabaseName();
        this.tableUniqueName = tableInfo.getTableUniqueName();
        this.factTableName = tableInfo.getFactTable().getTableName();
        fillDimensionsAndMeasuresForTables(tableInfo.getFactTable());
    }

    private void fillDimensionsAndMeasuresForTables(TableSchema tableSchema) {
        List<CarbonDimension> dimensions = new ArrayList<CarbonDimension>();
        List<CarbonMeasure> measures = new ArrayList<CarbonMeasure>();
        this.tableDimensionsMap.put(tableSchema.getTableName(), dimensions);
        this.tableMeasuresMap.put(tableSchema.getTableName(), measures);
        int dimensionOrdinal = 0;
        int measureOrdinal = 0;
        List<ColumnSchema> listOfColumns = tableSchema.getListOfColumns();
        for (ColumnSchema columnSchema : listOfColumns) {
            if (columnSchema.isDimensionColumn()) {
                dimensions.add(new CarbonDimension(columnSchema, dimensionOrdinal++));
            } else {
                measures.add(new CarbonMeasure(columnSchema, measureOrdinal++));
            }
        }
    }

    /**
     * @return the databaseName
     */
    public String getDatabaseName() {
        return databaseName;
    }

    /**
     * @return the tabelName
     */
    public String getFactTableName() {
        return factTableName;
    }

    /**
     * @return the tableUniqueName
     */
    public String getTableUniqueName() {
        return tableUniqueName;
    }

    /**
     * @return the metaDataFilepath
     */
    public String getMetaDataFilepath() {
        return metaDataFilepath;
    }

    /**
     * @return the tableLastUpdatedTime
     */
    public long getTableLastUpdatedTime() {
        return tableLastUpdatedTime;
    }

    /**
     * to get the number of dimension present in the table
     *
     * @param tableName
     * @return number of dimension present the table
     */
    public int getNumberOfDimensions(String tableName) {
        return tableDimensionsMap.get(tableName).size();
    }

    /**
     * to get the number of measures present in the table
     *
     * @param tableName
     * @return number of measures present the table
     */
    public int getNumberOfMeasures(String tableName) {
        return tableMeasuresMap.get(tableName).size();
    }

    /**
     * to get the all dimension of a table
     *
     * @param tableName
     * @return all dimension of a table
     */
    public List<CarbonDimension> getDimensionByTableName(String tableName) {
        return tableDimensionsMap.get(tableName);
    }

    public CarbonDimension getDimensionByName(String tableName, String columnName) {
        List<CarbonDimension> dimensionList = tableDimensionsMap.get(tableName);
        for (CarbonDimension dims : dimensionList) {
            if (dims.getColName().equalsIgnoreCase(columnName)) ;
            {
                return dims;
            }
        }
        return null;
    }

}
