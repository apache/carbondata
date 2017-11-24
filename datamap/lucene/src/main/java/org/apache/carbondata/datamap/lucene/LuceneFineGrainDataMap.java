package org.apache.carbondata.datamap.lucene;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datamap.dev.DataMapModel;
import org.apache.carbondata.core.datamap.dev.fgdatamap.AbstractFineGrainDataMap;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.indexstore.Blocklet;
import org.apache.carbondata.core.indexstore.FineGrainBlocklet;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.filter.intf.ExpressionType;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.solr.store.hdfs.HdfsDirectory;

import java.io.IOException;
import java.util.*;

public class LuceneFineGrainDataMap extends AbstractFineGrainDataMap {

    final static public int BLOCKID_ID = 0;

    final static public int BLOCKLETID_ID = 1;

    final static public int PAGEID_ID = 2;

    final static public int ROWID_ID = 3;

    /**
     * log information
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(LuceneFineGrainDataMap.class.getName());

    /**
     * searcher object for this datamap
     */
    private IndexSearcher indexSearcher = null;

    /**
     * datamap name
     */
    private String dataMapName = null;

    /**
     * segment id
     */
    private String segmentId = null;

    /**
     * talbe identifier
     */
    private AbsoluteTableIdentifier tableIdentifier = null;

    /**
     * default max values to return
     */
    private static int MAX_RESULT_NUMBER = 100;

    /**
     * analyzer for lucene index
     */
    private Analyzer analyzer = null;

    public LuceneFineGrainDataMap(AbsoluteTableIdentifier tableIdentifier, String dataMapName, String segmentId, Analyzer analyzer) {
        this.analyzer = analyzer;
        this.dataMapName = dataMapName;
        this.segmentId = segmentId;
        this.tableIdentifier = tableIdentifier;
    }

    /**
     * It is called to load the data map to memory or to initialize it.
     *
     * @param dataMapModel
     */
    public void init(DataMapModel dataMapModel) throws MemoryException, IOException {
        /**
         * get this path from file path
         */
        Path indexPath = FileFactory.getPath(dataMapModel.getFilePath());

        LOGGER.info("Lucene index read path " + indexPath.toString());

        /**
         * get file system , use hdfs file system , realized in solr project
         */
        FileSystem fs = FileFactory.getFileSystem(indexPath);

        /**
         * check this path valid
         */
        if (!fs.exists(indexPath)) {
            String errorMessage = String.format("index directory %s not exists.", indexPath);
            LOGGER.error(errorMessage);
            throw new IOException(errorMessage);
        }

        if (!fs.isDirectory(indexPath)) {
            String errorMessage = String.format("error index path %s, must be directory", indexPath);
            LOGGER.error(errorMessage);
            throw new IOException(errorMessage);
        }

        /**
         * open this index path , use HDFS default configuration
         */
        Directory indexDir = new HdfsDirectory(indexPath, FileFactory.getConfiguration());

        IndexReader indexReader = DirectoryReader.open(indexDir);
        if (indexReader == null) {
            throw new RuntimeException("failed to create index reader object");
        }

        /**
         * create a index searcher object
         */
        indexSearcher = new IndexSearcher(indexReader);
    }


    /**
     * Prune the datamap with filter expression. It returns the list of
     * blocklets where these filters can exist.
     *
     * @param filterExp
     * @param segmentProperties
     * @return
     */
    public List prune(FilterResolverIntf filterExp, SegmentProperties segmentProperties) throws IOException {

        /**
         * convert filter expr into lucene list query
         */
        List <String> fields = new ArrayList <String>();

        /**
         * only for test , query all data
         */
        String strQuery = "*:*";

        String[] sFields = new String[fields.size()];
        fields.toArray(sFields);

        /**
         * get analyzer
         */
        if (analyzer == null) {
            analyzer = new StandardAnalyzer();
        }

        /**
         * use MultiFieldQueryParser to parser query
         */
        QueryParser queryParser = new MultiFieldQueryParser(sFields, analyzer);
        Query query;
        try {
            query = queryParser.parse(strQuery);
        } catch (ParseException e) {
            String errorMessage = String.format("failed to filter block with query %s, detail is %s", strQuery, e.getMessage());
            LOGGER.error(errorMessage);
            return null;
        }

        if (strQuery == null) {
            query = transformFilterExpress(filterExp);
        }

        /**
         * execute index search
         */
        TopDocs result;
        try {
            result = indexSearcher.search(query, MAX_RESULT_NUMBER);
        } catch (IOException e) {
            String errorMessage = String.format("failed to search lucene data, detail is %s", e.getMessage());
            LOGGER.error(errorMessage);
            throw new IOException(errorMessage);
        }


        /**
         * temporary data, delete duplicated data
         * Map<BlockId, Map<BlockletId, Map<PageId, Set<RowId>>>>
         */
        Map <String, Map <String, Map <Integer, Set <Integer>>>> mapBlocks =
                new HashMap <String, Map <String, Map <Integer, Set <Integer>>>>();

        for (ScoreDoc scoreDoc : result.scoreDocs) {
            /**
             * get a document
             */
            Document doc = indexSearcher.doc(scoreDoc.doc);

            /**
             * get all fields
             */
            List <IndexableField> fieldsInDoc = doc.getFields();

            /**
             * get this block id
             * Map<BlockId, Map<BlockletId, Map<PageId, Set<RowId>>>>
             */
            String blockId = fieldsInDoc.get(BLOCKID_ID).stringValue();
            Map <String, Map <Integer, Set <Integer>>> mapBlocklets = mapBlocks.get(blockId);
            if (mapBlocklets == null) {
                mapBlocklets = new HashMap <String, Map <Integer, Set <Integer>>>();
                mapBlocks.put(blockId, mapBlocklets);
            }

            /**
             * get the blocklet id
             * Map<BlockletId, Map<PageId, Set<RowId>>>
             */
            String blockletId = fieldsInDoc.get(BLOCKLETID_ID).stringValue();
            Map <Integer, Set <Integer>> mapPageIds = mapBlocklets.get(blockletId);
            if (mapPageIds == null) {
                mapPageIds = new HashMap <Integer, Set <Integer>>();
                mapBlocklets.put(blockletId, mapPageIds);
            }

            /**
             * get the page id
             *  Map<PageId, Set<RowId>>
             */
            Number pageId = fieldsInDoc.get(PAGEID_ID).numericValue();
            Set <Integer> setRowId = mapPageIds.get(pageId.intValue());
            if (setRowId == null) {
                setRowId = new HashSet <Integer>();
                mapPageIds.put(pageId.intValue(), setRowId);
            }

            /**
             * get the row id
             * Set<RowId>
             */
            Number rowId = fieldsInDoc.get(ROWID_ID).numericValue();
            setRowId.add(rowId.intValue());
        }


        /**
         * result blocklets
         */
        List <Blocklet> blocklets = new ArrayList <Blocklet>();

        /**
         * transform all blocks into result type blocklets
         * Map<BlockId, Map<BlockletId, Map<PageId, Set<RowId>>>>
         */
        for (Map.Entry <String, Map <String, Map <Integer, Set <Integer>>>> mapBlock : mapBlocks.entrySet()) {
            String blockId = mapBlock.getKey();
            Map <String, Map <Integer, Set <Integer>>> mapBlocklets = mapBlock.getValue();
            /**
             * for blocklets in this block
             * Map<BlockletId, Map<PageId, Set<RowId>>>
             */
            for (Map.Entry <String, Map <Integer, Set <Integer>>> mapBlocklet : mapBlocklets.entrySet()) {
                String blockletId = mapBlocklet.getKey();
                Map <Integer, Set <Integer>> mapPageIds = mapBlocklet.getValue();
                List <FineGrainBlocklet.Page> pages = new ArrayList <FineGrainBlocklet.Page>();

                /**
                 * for pages in this blocklet
                 *  Map<PageId, Set<RowId>>>
                 */
                for (Map.Entry <Integer, Set <Integer>> mapPageId : mapPageIds.entrySet()) {
                    /**
                     * construct array rowid
                     */
                    int[] rowIds = new int[mapPageId.getValue().size()];
                    int i = 0;
                    /**
                     * for rowids in this page
                     * Set<RowId>
                     */
                    for (Integer rowid : mapPageId.getValue()) {
                        rowIds[i++] = rowid;
                    }
                    /**
                     * construct one page
                     */
                    FineGrainBlocklet.Page page = new FineGrainBlocklet.Page();
                    page.setPageId(mapPageId.getKey());
                    page.setRowId(rowIds);

                    /**
                     * add this page into list pages
                     */
                    pages.add(page);
                }

                /**
                 * add a FineGrainBlocklet
                 */
                blocklets.add(new FineGrainBlocklet(blockId, blockletId, pages));
            }
        }

        return blocklets;
    }

    private static Query transformFilterExpress(FilterResolverIntf filterExp) {
        FilterExpParser parser = new FilterExpParser(null, new StandardAnalyzer());
        Query query = null;
        try {
            query = parser.parserFilterExpr(filterExp);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return query;
    }


    /**
     * Clear complete index table and release memory.
     */
    public void clear() {

    }

    public static class FilterExpParser extends QueryParser {
        private int conj = CONJ_NONE;
        private int mods = MOD_NONE;
        static final int CONJ_NONE = 0;
        static final int CONJ_AND = 1;
        static final int CONJ_OR = 2;
        static final int MOD_NONE = 0;
        static final int MOD_NOT = 10;
        static final int MOD_REQ = 11;

        public FilterExpParser(String field, Analyzer analyzer) {
            super(field == null ? new String() : field, analyzer);
        }

        private void walkFilterExpr(List <BooleanClause> clauses, Expression expression) {
            Query q = null;
            if (expression == null) {
                return;
            }

            ExpressionType type = expression.getFilterExpressionType();
            switch (type) {
                case AND:
                    conj = CONJ_AND;
                case OR:
                    conj = CONJ_OR;
                case NOT:
                    mods = MOD_NOT;
                case EQUALS:
                case NOT_EQUALS:
                case LESSTHAN:
                case LESSTHAN_EQUALTO:
                case GREATERTHAN:
                case GREATERTHAN_EQUALTO:
                case ADD:
                case SUBSTRACT:
                case DIVIDE:
                case MULTIPLY:
                case IN:
                case LIST:
                case NOT_IN:
                case UNKNOWN:
                case LITERAL:
                case RANGE:
                case FALSE:
                case TRUE:
            }


            addClause(clauses, conj, mods, q);
        }

        public Query parserFilterExpr(FilterResolverIntf filterExp) throws ParseException {
            List <BooleanClause> clauses = new ArrayList <BooleanClause>();

            walkFilterExpr(clauses, filterExp.getFilterExpression());

            if (clauses.size() == 1) {
                return clauses.get(0).getQuery();
            } else {
                return getBooleanQuery(clauses);
            }
        }
    }
}
