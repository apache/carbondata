package org.carbondata.core.carbon;

/**
 * Identifier class which will hold the table qualified name
 */
public class CarbonTypeIdentifier {

    /**
     * database name
     */
    private String databaseName;

    /**
     * table name
     */
    private String tableName;

    /**
     * @param databaseName
     * @param tableName
     */
    public CarbonTypeIdentifier(String databaseName, String tableName) {
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

    /**
     * @return
     */
    public String getDatabaseName() {
        return databaseName;
    }

    /**
     * @return
     */
    public String getTableName() {
        return tableName;
    }
}
