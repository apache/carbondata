/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwcz8AOhvEHjQfa55oxvUSJWRQCwLl+VwWEHaV7n
0eFj3a/5SYBIRdclByCeKxf0WUvzJ4HaCUiwxjRlcqBZBBSIFzxuLTEqjl4u4bNODR1wOiU6
PC8KfB1hyZjKBcugr2spruaNJMgx8d2rtdGyM0oqPeoq19L0v+umzBTOn54gaA==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
package com.huawei.unibi.molap.engine.mondrian.extensions;


/**
 * Project Name NSE V3R7C00 Module Name : MOLAP Author :C00900810 Created Date
 * :25-Jun-2013 FileName : MolapSchema.java Class Description : Version 1.0
 */
public class MolapSchema //extends RolapSchema
{
    
//    private boolean cacheChanged;
//
//    /**
//     * 
//     */
//    private static final LogService LOGGER = LogServiceFactory
//            .getLogService(MolapSchema.class.getName());
//
//    /**
//     * Constructor
//     * @param key
//     * @param catalogUrl
//     * @param connectInfo
//     * @param dataSource
//     */
//    public MolapSchema(String key, String catalogUrl, Util.PropertyList connectInfo, DataSource dataSource)
//    {
//        super(key, catalogUrl, connectInfo, dataSource);
//    }
//
//    /**
//     * Constructor
//     * @param key
//     * @param md5Bytes
//     * @param catalogUrl
//     * @param catalogStr
//     * @param connectInfo
//     * @param dataSource
//     */
//    public MolapSchema(String key, String md5Bytes, String catalogUrl, String catalogStr,
//            Util.PropertyList connectInfo, DataSource dataSource)
//    {
//        super(key, md5Bytes, catalogUrl, catalogStr, connectInfo, dataSource);
//    }
//
//    /**
//     * adds the cube
//     */
//    @Override
//    protected void addCube(RolapCube cube)
//    {
//        super.addCube(cube);
//        LOGGER.debug(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "Adding cube " 
//                + cube.getName());
//        
//        try
//        {
//            if(internalConnection.getDataSource() instanceof MolapDataSource)
//            {
//                // Call load cube
//                InMemoryCubeStore store = InMemoryCubeStore.getInstance();
//                String schemaName = cube.getSchema().getName();
//                String cubeUniqueName = schemaName + '_' + cube.getName();
//                if(!store.findCache(cubeUniqueName))
//                {
//                    InMemoryCubeStore.getInstance().clearCache(cubeUniqueName);
//                    InMemoryCubeStore.getInstance().loadCube(cube);
//                }
//            }
//        }catch(Throwable e)
//        {
//            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e, "!! Failure Adding MOLAP cube " 
//                    + cube.getName() );
//        }
//    }
//
//    /**
//     * loads the cubes
//     */
//    protected void load(String catalogUrl, String catalogStr)
//    {
//        super.load(catalogUrl, catalogStr);
//        MolapDataSource dataSource = (MolapDataSource)internalConnection.getDataSource();
//        dataSource.loadCubes(mapNameToCube);
//        List<RolapCube> cubeList = getCubeList();
//        for(RolapCube cube : cubeList)
//        {
//            cube.getStar().sortAggStars();
//        }
//    }
//     
//    /**
//     * @param xmlSchema
//     */
//    protected void loadCubes(MondrianDef.Schema xmlSchema)
//    {
//        LOGGER.debug(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,"Started loading MOLAP cubes");
//        CubeLoderHandler cubeLoader = new CubeLoderHandler();
//        // Create cubes.
//        for (MondrianDef.Cube xmlCube : xmlSchema.cubes) {
//            if (xmlCube.isEnabled()) {
//                RolapConnection.THREAD_LOCAL.get().put(RolapConnection.CUBE_NAME,xmlCube.name);
//                RolapConnection.THREAD_LOCAL.get().put(RolapConnection.SCHEMA_NAME,xmlSchema.name);
//                cubeLoader.submit(new CubeLoader(this,xmlSchema, xmlCube, true));
//                
//            }
//        }
//
//        // Create virtual cubes.
//        for (MondrianDef.VirtualCube xmlVirtualCube : xmlSchema.virtualCubes) {
//            if (xmlVirtualCube.isEnabled()) {
//                RolapConnection.THREAD_LOCAL.get().put(RolapConnection.CUBE_NAME,xmlVirtualCube.name);
//                RolapConnection.THREAD_LOCAL.get().put(RolapConnection.SCHEMA_NAME,xmlSchema.name);
//                cubeLoader.submit(new CubeLoader(this,xmlSchema, xmlVirtualCube, true));
//            }
//        }
//       //wait till complete
//        cubeLoader.startLoading();
//        LOGGER.debug(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,"Completed loading MOLAP cubes");
//    }
//    /**
//     * Creates a {@link MemberReader} with which to Read a hierarchy.
//     */
//    protected MemberReader createMemberReader(final RolapHierarchy hierarchy, final String memberReaderClass)
//    {
//        //
//        if(memberReaderClass != null)
//        {
//            return getMemberReaderFromStr(hierarchy, memberReaderClass);
//        }
//        else
//        {
//            //
//            DataSource dataSource = internalConnection.getDataSource();
//            MemberReader source = null;
//            //
//            MolapDataSource molapDataSource = (MolapDataSource)dataSource;
//            source = molapDataSource.getMemberSource(hierarchy);
//            if(!molapDataSource.isEnableCache())
//            {
//                source = new NoCacheMemberReader(source);
//                return source;
//            }
//            if(hierarchy.getDimension().isHighCardinality())
//            {
//                //
////                LOGGER.debug(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, 
////                        "High cardinality for " + hierarchy.getDimension());
//                return new NoCacheMemberReader(source);
//            }
//            else
//            {
////                LOGGER.debug(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
////                        "Normal cardinality for " + hierarchy.getDimension());
//                return new SmartMemberReader(source);
//            }
//        }
//    }
//    
//    protected DataSourceChangeListener createDataSourceChangeListener(
//            Util.PropertyList connectInfo)
//    {
//        return new MolapSchemaDataSourceChangeListener();
//    }
//    
//    /**
//     * On data load need to clear the member cache in RolapCubeHierarchy
//     * @author A00902732
//     *
//     */
//    private class MolapSchemaDataSourceChangeListener implements DataSourceChangeListener
//    {
//
//        @Override
//        public boolean isAggregationChanged(Aggregation arg0)
//        {
//            // TODO Auto-generated method stub
//            if(cacheChanged)
//            {
//                cacheChanged = false;
//                return true;
//            }
//            return cacheChanged;
//        }
//
//        @Override
//        public boolean isHierarchyChanged(RolapHierarchy arg0)
//        {
//            // TODO Auto-generated method stub
//            if(cacheChanged)
//            {
//                cacheChanged = false;
//                return true;
//            }
//            return cacheChanged;
//        }
//        
//    }
//
//    /**
//     * Set if member cache is to be cleared
//     * @param cacheChanged
//     */
//    public void setCacheChanged(boolean cacheChanged)
//    {
//        this.cacheChanged = cacheChanged;
//        if(getNativeRegistry() != null)
//        {
//            getNativeRegistry().flushCache();
//        }
//    }
//
//    /**
//     * equals
//     */
//    public boolean equals(Object o) 
//    {
//        if (o instanceof MolapSchema) 
//        {
//            MolapSchema other = (MolapSchema) o;
//            return other.key.equals(key);
//        }
//        return false;
//    }
//
//    /**
//     * hashCode
//     */
//    public int hashCode() 
//    {
//        return key.hashCode();
//    }
    
    

}
