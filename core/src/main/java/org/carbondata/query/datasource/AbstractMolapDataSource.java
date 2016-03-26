/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.carbondata.query.datasource;

/**
 * Class Description : The data source process related to molap engine will be handled in this class
 * Version 1.0
 */
public abstract class AbstractMolapDataSource //implements MolapDataSource
{
    //    /**
    //     * This method will be used to get the MolapMemberSource for hierarchy
    //     *
    //     * @param hierarchy
    //     * @return MolapMemberSource
    //     *
    //     */
    //    @Override
    //    public SqlMemberSource getMemberSource(RolapHierarchy hierarchy)
    //    {
    //        return new MolapMemberSource(hierarchy);
    //    }
    //
    //    /**
    //     * This method will return the MolapTupleReader instance
    //     *
    //     * @param constraint
    //     *          TupleConstraint
    //     * @param schema
    //     *          RolapSchema
    //     *
    //     */
    //    @Override
    //    public SqlTupleReader getTupleReader(TupleConstraint constraint, RolapSchema schema)
    //    {
    //        return new MolapTupleReader(constraint, schema);
    //    }
    //
    //    /**
    //     * This method will be used to get the dimension cardinality
    //     *
    //     * @param name
    //     *          dimension name
    //     * @param start
    //     *          Rolap star
    //     *
    //     */
    //    @Override
    //    public int getDimCardinality(String name, RolapStar star)
    //    {
    //        MolapStatement statement = new MolapStatement(this);
    //        statement.executeCount(name, star);
    //        int count = 0;
    //        if(statement.getIterator().isNext())
    //        {
    //            count = ((Double)statement.getIterator().getObject(1)).intValue();
    //        }
    //        return count;
    //    }
    //
    //    /**
    //     * This method will be used to get the MolapSegmentLoader instance
    //     * @return MolapSegmentLoader
    //     *
    //     */
    //    @Override
    //    public SegmentLoader getSegmentLoader()
    //    {
    //        return new MolapSegmentLoader();
    //    }
    //
    //    /**
    //     * This method returns the aggregate count
    //     * @param table
    //     *          table name
    //     * @param star
    //     *         RolapStar
    //     * @return aggregate count
    //     *
    //     */
    //    @Override
    //    public int getAggCount(String table, RolapStar star)
    //    {
    //        MolapStatement statement = new MolapStatement(this);
    //        statement.executeAggCount(table, star);
    //        int count = 0;
    //        if(statement.getIterator().isNext())
    //        {
    //            ++statement.rowCount;
    //            count = ((Double)statement.getIterator().getObject(1)).intValue();
    //        }
    //        return count;
    //    }
    //
    //    /**
    //     * This method will return table information as a string array
    //     *
    //     * @return table name array
    //     *
    //     */
    //    @Override
    //    public String[] getMetaTables()
    //    {
    //        String schemaName = (String)RolapConnection.THREAD_LOCAL.get().get(RolapConnection.SCHEMA_NAME);
    //        String[] s = new String[0];
    //        if(schemaName == null)
    //        {
    //            return s;
    //        }
    //
    //        List<Cube> cubes = MolapMetadata.getInstance().getCubesStartWith(schemaName);
    //        if(cubes.size() == 0)
    //        {
    //            return s;
    //        }
    //        List<String> tableNames = new ArrayList<String>();
    //        for(Cube cube : cubes)
    //        {
    //            tableNames.addAll(cube.getMetaTableNames());
    //        }
    //        return tableNames.toArray(new String[tableNames.size()]);
    //    }
    //
    //    /**
    //     * This method will return column set presentin the table
    //     *
    //     * @param table
    //     *          table name
    //     * @return table set
    //     *
    //     */
    //    @Override
    //    public Set<String> getColumns(String table)
    //    {
    //        String schemaName = (String)RolapConnection.THREAD_LOCAL.get().get(RolapConnection.SCHEMA_NAME);
    //        if(schemaName == null)
    //        {
    //            return null;
    //        }
    //
    //        List<Cube> cubes = MolapMetadata.getInstance().getCubesStartWith(schemaName);
    //        if(cubes.size() == 0)
    //        {
    //            return null;
    //        }
    //
    //        for(Cube cube : cubes)
    //        {
    //            Set<String> columnsForAgg = cube.getMetaTableColumnsForAgg(table);
    //            if(columnsForAgg != null)
    //            {
    //                return columnsForAgg;
    //            }
    //        }
    //
    //        return null;
    //    }
    //
    //    /**
    //     * This method will return Molap dialect
    //     *
    //     * @return MolapDummyDialect
    //     *
    //     */
    //    @Override
    //    public Dialect getDialect()
    //    {
    //        return new MolapDummyDialect();
    //    }
    //
    //    /**
    //     * This method will load the molap cube
    //     *
    //     * @param cubes
    //     *          Schema name and Cube details
    //     *
    //     */
    //    @Override
    //    public void loadCubes(Map<String, RolapCube> cubes)
    //    {
    //        MolapMetadata.getInstance().load(cubes);
    //    }
    //
    //    /**
    //     * Retrieves the log writer for this <code>DataSource</code>
    //     * object.
    //     *
    //     * @return  PrintWriter
    //     *          Log Writer
    //     *@exception SQLException if a database access error occurs
    //     */
    //    @Override
    //    public PrintWriter getLogWriter() throws SQLException
    //    {
    //        return null;
    //    }
    //
    //    /**
    //     * Sets the log writer for this <code>DataSource</code> object to the given
    //     * <code>java.io.PrintWriter</code> object.
    //     *
    //     * @param PrintWriter
    //     *            Log Writer
    //     * @exception SQLException
    //     *                if a database access error occurs
    //     */
    //    @Override
    //    public void setLogWriter(PrintWriter out) throws SQLException
    //    {
    //
    //    }
    //
    //    @Override
    //    public void setLoginTimeout(int seconds) throws SQLException
    //    {
    //
    //    }
    //
    //    @Override
    //    public int getLoginTimeout() throws SQLException
    //    {
    //        return 0;
    //    }
    //
    //    public <T> T unwrap(Class<T> iface) throws SQLException
    //    {
    //        return null;
    //    }
    //
    //    public boolean isWrapperFor(Class<?> iface) throws SQLException
    //    {
    //        return false;
    //    }
    //
    //    @Override
    //    public Connection getConnection() throws SQLException
    //    {
    //        return null;
    //    }
    //
    //    @Override
    //    public Connection getConnection(String username, String password) throws SQLException
    //    {
    //        return null;
    //    }
    //
    //    @Override
    //    public boolean flushCube(String schemaName,String cubeName)
    //    {
    //        return MolapCacheManager.getInstance().flushCube(schemaName, cubeName);
    //    }
}
