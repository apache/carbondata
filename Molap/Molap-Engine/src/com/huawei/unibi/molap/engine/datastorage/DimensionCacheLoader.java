/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwdh/HjOjN0Brs7b7TRorj6S6iAIeaqK90lj7BAM
GSGxBngRbz/XGYymwzmyXy0KWkvivNKdHwo/qSoHqlJQpYIhlQ1Oqghe3jOLOJrNij3LjWE0
ZBhhJhq3kdXETkghBqALRIsarMQJz17aMhkMULoGWUn6YDhQHbU9VRbBHHnwXw==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * 
 */
package com.huawei.unibi.molap.engine.datastorage;

import java.io.File;
import java.io.IOException;
import java.util.List;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
//import com.huawei.unibi.molap.datastorage.store.FileHolder;
import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFile;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory;
import com.huawei.unibi.molap.engine.datastorage.streams.DataInputStream;
//import com.huawei.unibi.molap.engine.scanner.Scanner;
//import com.huawei.unibi.molap.engine.scanner.impl.KeyValue;
import com.huawei.unibi.molap.engine.util.CacheUtil;
import com.huawei.unibi.molap.engine.util.MolapDataInputStreamFactory;
import com.huawei.unibi.molap.engine.util.MolapEngineLogEvent;
import com.huawei.unibi.molap.keygenerator.KeyGenerator;
import com.huawei.unibi.molap.keygenerator.factory.KeyGeneratorFactory;
import com.huawei.unibi.molap.olap.MolapDef;

/**
 * @author R00900208
 * 
 */
public final class DimensionCacheLoader
{
    /**
     * Attribute for Molap LOGGER
     */
    private static final LogService LOGGER = LogServiceFactory
            .getLogService(DimensionCacheLoader.class.getName());
    
    private DimensionCacheLoader()
    {
        
    }
    
    /**
     * @param hierC
     * @param fileStore
     * @throws IOException
     */
    public static void loadHierarichyFromFileStore(HierarchyStore hierC, String fileStore) throws IOException
    {
        loadBTreeHierarchyFileStore(hierC, fileStore);
    }

    /**
     * @param hierC
     * @param datasource
     * @throws IOException
     */
    /*public static void loadHierarichyFromDataSource(HierarchyStore hierC, DataSource datasource) throws IOException
    {
        long t1 = System.currentTimeMillis();

        String storeLocation = ((MolapDataSourceImpl)datasource).getFileStore();
        String inMemoryURL = ((MolapDataSourceImpl)datasource).getURL();

        if(storeLocation != null && inMemoryURL == null)
        {
            // loadHierarchyFileStore(hierC, tableNames, schemaName, cubeName);
            loadBTreeHierarchyFileStore(hierC, storeLocation);
        }
        else
        {
            // Load from MySQlDB
            loadHierarchyMySql(hierC, datasource);
        }

        long t2 = System.currentTimeMillis();

        LOGGER.debug(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, 
                "Hier build time is : " + 
                (t2 - t1) + " (MS) for : " + hierC.getHierName());
    }*/

//    /**
//     * @param targetStore
//     * @param slices
//     * @param fileStore
//     */
//    public static void loadHierarchyFromSlice(HierarchyStore targetStore, List<HierarchyStore> slices, String fileStore)
//    {
//        KeyGenerator keyGenerator = getKeyGenerator(targetStore.getRolapHierarchy());
//        //
//        List<Scanner> scanners = new ArrayList<Scanner>();
//
//        // Make scanners from old slices
//        for(HierarchyStore store : slices)
//        {
//            //
//            KeyValue keyValue = new KeyValue();
//            Scanner scanner = new NonFilterTreeScanner(new byte[0], null, keyGenerator, keyValue, null, null);
//            if(store.getHierBTreeStore().size() > 0)
//            {
//                //
//                store.getHierBTreeStore().getNext(new byte[0], scanner);
//                scanners.add(scanner);
//            }
//        }
//
//        // Make scanner from file store
//        DataInputStream factStream = null;
//
//        if(fileStore != null)
//        {
//            
//            RelationOrJoin relation = targetStore.getRolapHierarchy().getRelation();
//            String tableName = relation==null?targetStore.getDimensionName():((Table)relation).name;
//            String fileName = tableName.replaceAll(" ", "_") + '_' + targetStore.getTableName();
//            MolapFile file = FileFactory.getMolapFile(fileStore, FileFactory.getFileType());
//            if(file.isDirectory())
//            {
//                // Read from the base location.
//                String baseLocation = fileStore + File.separator + fileName;
//                MolapFile baseFile = FileFactory.getMolapFile(baseLocation, FileFactory.getFileType());
//                try
//                {
//                    if(FileFactory.isFileExist(baseLocation, FileFactory.getFileType(baseLocation)))
//                    {
//                        factStream = MolapDataInputStreamFactory.getDataInputStream(baseLocation, keyGenerator.getKeySizeInBytes(), 0, false, fileStore, fileName,FileFactory.getFileType());
//                        factStream.initInput();
//
//                        scanners.add(new DataInputStreamWrappedScanner(factStream));
//                    }
//                }
//                catch(IOException e)
//                {
//                    // TODO Auto-generated catch block
//                    e.printStackTrace();
//                }
//            }
//        }
//
//        ScannersInputCombiner inputStream = new ScannersInputCombiner(scanners, keyGenerator, new ArrayList<String>(),
//                false);
//        inputStream.initInput();
//
//        targetStore.build(keyGenerator, inputStream);
//
//        if(factStream != null)
//        {
//            factStream.closeInput();
//        }
//
//    }

    /**
     * DataInputStream is wrapped as a Scanner.
     * 
     * @author K00900207
     * 
     */
//    private static class DataInputStreamWrappedScanner implements Scanner
//    {
//        /**
//         * 
//         */
//        private DataInputStream dataInputStream;
//
//        //
//        DataInputStreamWrappedScanner(DataInputStream inputStream)
//        {
//            this.dataInputStream = inputStream;
//        }
//
//        /**
//         * 
//         */
//        private KeyValue data;
//
//        @Override
//        public KeyValue getNext()
//        {
//            return data;
//        }
//
//        @Override
//        public boolean isDone()
//        {
//            Pair<byte[], double[]> pair = dataInputStream.getNextHierTuple();
//            data = new KeyValue();
//            // Check style approved
//            if(pair == null)
//            {
//                return true;
//            }
//            data.setOrigainalValue(pair.getValue());
//            data.setOriginalKey(pair.getKey());
//
//            return false;
//        }
//
//        @Override
//        public void setDataStore(DataStore dataStore, DataStoreBlock block, int currIndex)
//        {
//        }
//
//        public void closeInput()
//        {
//            dataInputStream.closeInput();
//        }
//
//        @Override
//        public FileHolder getFileHolder()
//        {
//            return null;
//        }
//    }

    /**
     * @param hierC
     * @param fileStore
     * @throws IOException
     */
    private static void loadBTreeHierarchyFileStore(HierarchyStore hierC, String fileStore) throws IOException
    {
        KeyGenerator keyGen = getKeyGenerator(hierC.getRolapHierarchy());
        //
        
//        String tableName = relation==null?hierC.getDimensionName():((Table)relation).name;
//        String tableName = "";
//        if(relation == null)
//        {
//            tableName = hierC.getDimensionName();
//        }
//        else
//        {
//            tableName = relation.toString();
//        }
//        
//        if(tableName.contains("."))
//        {
//            tableName = tableName.split("\\.")[1];
//        }
//        if(hierC.getFactTableName().equals(tableName))
//        {
//            tableName = hierC.getDimensionName();
//        }
        
        String fileName = hierC.getDimensionName().replaceAll(" ", "_") + '_' + hierC.getTableName();
        //
        MolapFile file = FileFactory.getMolapFile(fileStore, FileFactory.getFileType(fileStore));
        if(file.isDirectory())
        {
            // Read from the base location.
            String baseLocation = fileStore + File.separator + fileName;
//            MolapFile baseFile = FileFactory.getMolapFile(baseLocation, FileFactory.getFileType());
            if(FileFactory.isFileExist(baseLocation, FileFactory.getFileType(baseLocation)))
            {
                //
                DataInputStream factStream = MolapDataInputStreamFactory.getDataInputStream(baseLocation, keyGen.getKeySizeInBytes(), 0,
                         false, fileStore, fileName,FileFactory.getFileType(fileStore));
                factStream.initInput();
                hierC.build(keyGen, factStream);

                factStream.closeInput();
            }
        }
    }


    private static KeyGenerator getKeyGenerator(MolapDef.Hierarchy rolapHierarchy)
    {
        MolapDef.Level[] levels = rolapHierarchy.levels;
        int levelSize = levels.length;
        int[] lens = new int[levelSize];
        int i = 0;
        for(MolapDef.Level level : levels)
        {
//            if(!level.isAll())
//            {
                lens[i++] = level.levelCardinality;
               // lens[i++] = Long.toBinaryString((((RolapLevel)level).levelCardinality)).length();
//            }
        }

        return KeyGeneratorFactory.getKeyGenerator(lens);
       // return new MultiDimKeyVarLengthGenerator(lens);
    }

   /* private static void loadHierarchyMySql(HierarchyStore hierC, DataSource datasource) throws IOException
    {
        KeyGenerator keyGen = getKeyGenerator(hierC.getRolapHierarchy());

        String query = "SELECT DISTINCT ID FROM " + hierC.getTableName() + " ORDER BY ID";

        Cube inputMetaCube = new Cube(hierC.getTableName());

        DBDataInputStream dataInputStream = new DBDataInputStream(datasource, inputMetaCube, query,
                hierC.getTableName(), new ArrayList<String>(), false, "");
        dataInputStream.initInput();
        hierC.build(keyGen, dataInputStream);
    }*/

//    public static void loadMemberFromSlices(MemberStore levelCache, List<MemberStore> slices, String fileStore, String dataType, String factTableName)
//    {
//        if(fileStore != null)
//        {
//            loadMemberFromFileStore(levelCache, fileStore,dataType,factTableName);
//        }
//
//        for(MemberStore slice : slices)
//        {
//            levelCache.mergeStore(slice);
//        }
//        levelCache.createSortIndex();
//    }

    /**
     * Loads members from file store.
     * 
     * @param levelCache
     * @param fileStore
     * @param factTablename 
     */
    public static void loadMemberFromFileStore(MemberStore levelCache, String fileStore, String dataType,
            String factTablename, String tableName)
    {
        //
        Member [][] members = null;
        
        int[] globalSurrogateMapping = null;
//        Map<Integer,Integer> globalSurrogate = new HashMap<Integer,Integer>();
        int minValue=0;
        int minValueForLevelFile=0;
        int maxValueFOrLevelFile=0;
        List<int[][]> sortOrderAndReverseOrderIndex= null;
//        RelationOrJoin relation = levelCache.getRolapLevel().getHierarchy().getRelation();
//        String tableName = relation==null? levelCache.getRolapLevel().getDimension().getName():((Table)relation).name;
        //
//        String tableName = "";
//        if(relation == null)
//        {
//            tableName = levelCache.getRolapLevel().getDimension().getName();
//        }
//        else
//        {
//            tableName = relation.toString();
//        }
        
        if(tableName.contains("."))
        {
            tableName = tableName.split("\\.")[1];
        }
        if(factTablename.equals(tableName))
        {
            tableName = factTablename;
        }


        String fileName = tableName + '_' + levelCache.getColumnName();
        //
        MolapFile file = FileFactory.getMolapFile(fileStore, FileFactory.getFileType(fileStore));
        if(file.isDirectory())
        {
            // Read from the base location.
            String baseLocation = fileStore + File.separator + fileName + MolapCommonConstants.LEVEL_FILE_EXTENSION;
            String baseLocationForGlobalKeys = fileStore + File.separator + fileName + ".globallevel";
            String baseLocationForsortIndex = fileStore + File.separator + fileName;
//            MolapFile baseFile = FileFactory.getMolapFile(baseLocation, FileFactory.getFileType());
            try
            {
                if(FileFactory.isFileExist(baseLocation, FileFactory.getFileType(baseLocation)))
                {
                     members = CacheUtil.getMembersList(baseLocation, (byte)-1,dataType);
                     minValueForLevelFile=CacheUtil.getMinValueFromLevelFile(baseLocation);
                     maxValueFOrLevelFile=CacheUtil.getMaxValueFromLevelFile(baseLocation);
                    minValue=CacheUtil.getMinValue(baseLocationForGlobalKeys);
                    
                    globalSurrogateMapping = CacheUtil.getGlobalSurrogateMapping(baseLocationForGlobalKeys);
                    if(null!=members)
                    {
                        sortOrderAndReverseOrderIndex=CacheUtil.getLevelSortOrderAndReverseIndex(baseLocationForsortIndex);
                    }
                }
            }
            catch(IOException e)
            {
                LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e);
            }
        }

        levelCache.addAll(members,minValueForLevelFile,maxValueFOrLevelFile,sortOrderAndReverseOrderIndex);
        levelCache.addGlobalKey(globalSurrogateMapping,minValue);
    }

//    public static void loadMembersFromDataSource(MemberStore levelCache, DataSource datasource) throws IOException
//    {
//        RolapProperty[] properties = levelCache.getRolapLevel().getProperties();
//        StringBuffer buffer = new StringBuffer();
//        buffer.append("SELECT DISTINCT NAME, ID ");
//        //
//        boolean hasOrdinalCol = hasOrdinalColumn(levelCache.getRolapLevel());
//
//        if(hasOrdinalCol)
//        {
//            buffer.append(", ")
//                    .append(((MondrianDef.Column)levelCache.getRolapLevel().getOrdinalExp()).getColumnName());
//            buffer.append(" ");
//        }
//        //
//        for(RolapProperty property : properties)
//        {
//            buffer.append(" , ").append(((MondrianDef.Column)property.getExp()).getColumnName());
//            buffer.append(" ");
//        }
//
//        buffer.append(" FROM ");
//        // buffer.append(" \"");
//        buffer.append(levelCache.getTableForMember());
//        // buffer.append("\" ");
//       //final String query = buffer.toString();
//        //
//        ResultSet resultSet = null;
//        Statement stmt = null;
//        Connection conn= null;
//        try
//        {
//            conn = datasource.getConnection();
//            /**
//             * Fortify fix: NULL_DEREFRENCE
//             */
//            if(null != conn)
//            {
//                stmt = conn.createStatement();
//                resultSet = stmt.executeQuery(buffer.toString());
//                //
//                while(resultSet.next())
//                {
//                    String name = resultSet.getString(1);
//                    int id = resultSet.getInt(2);
//                    // byte[] id = resultSet.getBytes(2);
//    
//                    Member dim = null;
//                    if(levelCache.getNameColIndex() == -1)
//                    {
//                        dim = new Member((name == null) ? new char[0] : name.toCharArray());
//                    }
//                    else
//                    {
//                        dim = new NameColumnMember((name == null) ? new char[0] : name.toCharArray(),
//                                levelCache.getNameColIndex());
//                    }
//                    //
//                    int size = properties.length + (hasOrdinalCol ? 1 : 0);
//                    if(size > 0)
//                    {
//                        Object[] props = new Object[size];
//                        for(int i = 0;i < size;i++)
//                        {
//                            props[i] = resultSet.getObject(3 + i);
//                        }
//                        //
//                        dim.setAttributes(props);
//                    }
//                    levelCache.addMember(id, dim);
//                }
//            }
//            levelCache.createSortIndex();
//        }
//        catch(SQLException e)
//        {
//            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e);
//        }
//        finally
//        {
//            //
//            if(resultSet != null)
//            {
//                try
//                {
//                    resultSet.close();
//                }
//                catch(SQLException e)
//                {
//                    LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e);
//                }
//            }
//            
//            if(stmt != null)
//            {
//                try
//                {
//                    stmt.close();
//                }
//                catch(SQLException e)
//                {
//                    LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e);
//                }
//            }
//            
//            if(conn != null)
//            {
//                try
//                {
//                    conn.close();
//                }
//                catch(SQLException e)
//                {
//                    LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e);
//                }
//            }
//        }
//    }

    /**
     * 
     */
//    private static boolean assignOrderKeys = MondrianProperties.instance().CompareSiblingsByOrderKey.get();

//    /**
//     * Check whether to consider Ordinal column separately if it is configured.
//     */
//    private static boolean hasOrdinalColumn(MolapDef.Level level)
//    {
//        return (!level.getOrdinalExp().equals(level.getKeyExp()));
//    }
}
