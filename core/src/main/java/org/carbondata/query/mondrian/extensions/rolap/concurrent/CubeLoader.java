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

package org.carbondata.query.mondrian.extensions.rolap.concurrent;

import java.util.concurrent.Callable;

public class CubeLoader implements Callable<CubeLoader> {

    @Override
    public CubeLoader call() throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

    //    /**
    //     *
    //     */
    //    private mondrian.olap.MondrianDef.Schema xmlSchema;
    //    /**
    //     *
    //     */
    //    private mondrian.olap.MondrianDef.Cube xmlCube;
    //    /**
    //     *
    //     */
    //    private boolean load;
    //    /**
    //     *
    //     */
    //    private RolapSchema rolapSchema;
    //    /**
    //     *
    //     */
    //    private VirtualCube xmlVirtualCube;
    //    /**
    //     *
    //     */
    //    private boolean isCompleted;
    //
    //    /**
    //     *
    //     */
    //    private static final LogService LOGGER = LogServiceFactory
    //            .getLogService(CubeLoader.class.getName());
    //
    //    public CubeLoader(RolapSchema rolapSchema, mondrian.olap.MondrianDef.Schema xmlSchema,
    //            mondrian.olap.MondrianDef.Cube xmlCube, boolean load)
    //    {
    //        this.rolapSchema = rolapSchema;
    //        this.xmlSchema = xmlSchema;
    //        this.xmlCube = xmlCube;
    //        this.xmlVirtualCube =null;
    //        this.load = load;
    //    }
    //
    //    public CubeLoader(RolapSchema rolapSchema,
    //            mondrian.olap.MondrianDef.Schema xmlSchema,
    //            VirtualCube xmlVirtualCube, boolean load)
    //    {
    //        this.rolapSchema = rolapSchema;
    //        this.xmlSchema = xmlSchema;
    //        this.xmlCube = null;
    //        this.xmlVirtualCube = xmlVirtualCube;
    //        this.load = load;
    //    }
    //
    //    @Override
    //    public CubeLoader call() throws Exception
    //    {
    //        this.isCompleted =false;
    //        load();
    //        this.isCompleted =true;
    //        return this;
    //    }
    //    /**
    //     * Which tells the task is completed or not
    //     * @return
    //     */
    //    public boolean isCompleted()
    //    {
    //        return isCompleted;
    //    }
    //
    //    public void load() throws Exception
    //    {
    //        try
    //        {
    //            if( null != this.xmlCube)
    //            {
    //                //System.out.println("Loading normal cube : "+xmlCube.name);
    //                RolapCube cube = new RolapCube(rolapSchema,this.xmlSchema,this.xmlCube,this.load);
    //                Util.discard(cube);
    //                setCalculatedMeasures(cube);
    //            }
    //            else if( null != xmlVirtualCube)
    //            {
    //                //System.out.println("Loading Virtual cube : "+xmlVirtualCube.name);
    //                RolapCube cube = new RolapCube(rolapSchema,this.xmlSchema,this.xmlVirtualCube,this.load);
    //                Util.discard(cube);
    //                setCalculatedMeasures(cube);
    //            }
    //        }catch(Throwable e)
    //        {
    //            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,"Error in load method "+ e.getMessage());
    //            throw new Exception(e);
    //        }
    //    }
    //
    //    private void setCalculatedMeasures(RolapCube cube)
    //    {
    //    	Cube molapCube = MolapMetadata.getInstance().getCube(cube.getSchema().getName()+'_'+cube.getName());
    //    	List<RolapMember> measuresMembers = cube.getMeasuresMembers();
    //    	for(RolapMember rolapMember : measuresMembers)
    //    	{
    //    		if(rolapMember instanceof RolapCalculatedMeasure)
    //    		{
    //    			RolapCalculatedMeasure calculatedMeasure = (RolapCalculatedMeasure)rolapMember;
    //
    //    			CalculatedMeasure measure = new CalculatedMeasure(calculatedMeasure.getExpression(), calculatedMeasure.getName());
    //    			molapCube.getMeasures(molapCube.getFactTableName()).add(measure);
    //    		}
    //    	}
    //    }
    //
    //    @Override
    //    public String toString()
    //    {
    //        if( null != xmlCube && null !=xmlSchema)
    //        {
    //            return xmlSchema.name+':'+xmlCube.name;
    //        }else if( null != xmlVirtualCube && null != xmlSchema)
    //        {
    //            return xmlSchema.name+':'+xmlVirtualCube.name;
    //        }else
    //        {
    //            return "Not defined";
    //        }
    //
    //    }
}
