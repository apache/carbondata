package org.apache.carbondata.datamap.lucene;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datamap.DataMapDistributable;
import org.apache.carbondata.core.datamap.DataMapMeta;
import org.apache.carbondata.core.datamap.dev.DataMap;
import org.apache.carbondata.core.datamap.dev.DataMapFactory;
import org.apache.carbondata.core.datamap.dev.DataMapWriter;
import org.apache.carbondata.core.events.ChangeEvent;
import org.apache.carbondata.core.indexstore.schema.FilterType;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonMetadata;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.util.path.CarbonStorePath;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LuceneDataMapFactory implements DataMapFactory {
    /**
     * Logger
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(LuceneDataMapFactory.class.getName());

    /**
     * table's index columns
     */
    private DataMapMeta dataMapMeta = null;

    /**
     * index path to store index data
     */
    private String indexPath = null;

    /**
     * analyzer for lucene
     */
    private Analyzer analyzer = null;

    /**
     * index name
     */
    private String dataMapName = null;

    /**
     * table identifier
     */
    private AbsoluteTableIdentifier tableIdentifier = null;


    /**
     * Initialization of Datamap factory with the identifier and datamap name
     *
     * @param identifier
     * @param dataMapName
     */
    public void init(AbsoluteTableIdentifier identifier, String dataMapName) throws IOException {
        this.tableIdentifier = identifier;
        this.dataMapName = dataMapName;

        /**
         * get carbonmetadata from carbonmetadata instance
         */
        CarbonMetadata carbonMetadata = CarbonMetadata.getInstance();

        String tableUniqueName = identifier.getCarbonTableIdentifier().getTableUniqueName();

        /**
         * get carbon table
         */
        CarbonTable carbonTable = carbonMetadata.getCarbonTable(tableUniqueName);
        if (carbonTable == null) {
            String errorMessage = String.format("failed to get carbon table with name %s", tableUniqueName);
            LOGGER.error(errorMessage);
            throw new IOException(errorMessage);
        }

        TableInfo tableInfo = carbonTable.getTableInfo();
        List<ColumnSchema> lstCoumnSchemas = tableInfo.getFactTable().getListOfColumns();

        /**
         * add all columns into lucene indexer , TODO:only add index columns
         */
        List<String> indexedColumns = new ArrayList<String>();
        for (ColumnSchema columnSchema : lstCoumnSchemas) {
            if (!columnSchema.isInvisible()) {
                indexedColumns.add(columnSchema.getColumnName());
            }
        }

        /**
         * TODO: how to get idex path
         */
        FilterType optimizedOperation = FilterType.LIKE;
        this.dataMapMeta = new DataMapMeta(indexedColumns, optimizedOperation);

        /**
         * get analyzer  TODO: how to get analyzer ?
         */
        analyzer = new StandardAnalyzer();
    }

    /**
     * Return a new write for this datamap
     *
     * @param segmentId
     */
    public DataMapWriter createWriter(String segmentId) {
        return new LuceneDataMapWriter(tableIdentifier, getMeta(), dataMapName, segmentId);
    }

    /**
     * Get the datamap for segmentid
     *
     * @param segmentId
     */
    public List<DataMap> getDataMaps(String segmentId) throws IOException {
        List<DataMap> lstDataMap = new ArrayList<DataMap>();
        DataMap dataMap = new LuceneDataMap(tableIdentifier, dataMapName, segmentId, analyzer);
        CarbonTablePath tablePath = CarbonStorePath.getCarbonTablePath(tableIdentifier);
        String dataPath = tablePath.getCarbonDataDirectoryPath("0", segmentId);
        try {
            dataMap.init(dataPath + File.separator + dataMapName);
        } catch (MemoryException e) {
            LOGGER.error("failed to get lucene datamap , detail is {}" + e.getMessage());
            return lstDataMap;
        }
        lstDataMap.add(dataMap);
        return lstDataMap;
    }

    /**
     * Get datamap for distributable object.
     *
     * @param distributable
     */
    public DataMap getDataMap(DataMapDistributable distributable) {
        return new LuceneDataMap(tableIdentifier, dataMapName, distributable.getSegmentId(), analyzer);
    }

    /**
     * Get all distributable objects of a segmentid
     *
     * @param segmentId
     * @return
     */
    public List<DataMapDistributable> toDistributable(String segmentId) {
        List<DataMapDistributable> lstDataMapDistribute = new ArrayList<DataMapDistributable>();
        LuceneDataMapDistributable luceneDataMapDistributable = new LuceneDataMapDistributable();
        luceneDataMapDistributable.setDataMapFactoryClass(this.getClass().getName());
        luceneDataMapDistributable.setDataMapName(dataMapName);
        luceneDataMapDistributable.setSegmentId(segmentId);
        lstDataMapDistribute.add(luceneDataMapDistributable);
        return lstDataMapDistribute;
    }

    /**
     * @param event
     */
    public void fireEvent(ChangeEvent event) {

    }

    /**
     * Clears datamap of the segment
     *
     * @param segmentId
     */
    public void clear(String segmentId) {

    }

    /**
     * Clear all datamaps from memory
     */
    public void clear() {

    }

    /**
     * Return metadata of this datamap
     */
    public DataMapMeta getMeta() {
        return dataMapMeta;
    }
}
