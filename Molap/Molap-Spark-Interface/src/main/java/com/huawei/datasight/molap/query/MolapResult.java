package com.huawei.datasight.molap.query;


public class MolapResult //implements Result
{


//	MolapResultStreamHolder molapResultStreamHolder;
//	MolapQueryImpl molapQueryImpl; 
//	private RolapAxis slicerAxis;
//	Axis[] axes;
//	private ResultMappinDTO resultMappinDTO; 
//	
//	public MolapResult(MolapResultStreamHolder molapResultStreamHolder, MolapQueryImpl molapQueryImpl, ResultMappinDTO resultMappinDTO, int axisCount) {
//		super();
//		this.molapResultStreamHolder = molapResultStreamHolder;
//		this.resultMappinDTO = resultMappinDTO;
//		this.molapQueryImpl = molapQueryImpl;
//		
//		
//		axes = new Axis[axisCount];
////      this.slicerAxis = new RolapAxis(tupleList);
//		
//		TupleIterable colTupleIterable = new AbstractMColTupleIterable(molapResultStreamHolder.getResultStream());
//		this.axes[0] = new RolapAxis(TupleCollections.materialize(colTupleIterable, false));
//		if(axes.length>1)
//		{
//		    TupleIterable rowTupleIterable = new AbstractMTupleIterable(molapResultStreamHolder.getResultStream());
//		    this.axes[1] = new RolapAxis(TupleCollections.materialize(rowTupleIterable, false));
//		}
//		
//	}
//	
//	 /**
//	 * 
//	 * @return the Dynamic Levels identified in the query
//	 * 
//	 */
//	public  Map<String, Level> getDynamicLevels()
//	 {
//	     if(resultMappinDTO!=null)
//	     {
//	         return resultMappinDTO.dynamicLevels;
//	     }
//	     
//	     return null;
//	 }
//	 
//	@Override
//	public Query getQuery() {
//		return null;
//	}
//
//	@Override
//	public Axis[] getAxes() {
//		return axes;
//	}
//	
//	@Override
//	public Axis getSlicerAxis() {
//		return null;
//	}
//
//	@Override
//	public Cell getCell(int[] pos) {
//
//		Object value;
//		if(pos.length >1){
//			value = molapResultStreamHolder.getResultStream().getResult().getCell(pos[0], pos[1]);
//		} else {
//			value = molapResultStreamHolder.getResultStream().getResult().getCell(pos[0],0);
//		}
//		return new MolapCell(pos, value, this);
//	}
//
//	@Override
//	public void print(PrintWriter pw) {
//		// TODO Auto-generated method stub
//		
//	}
//
//	@Override
//	public void close() {
//		// TODO Auto-generated method stub
//		
//	}
//    /**
//     * Returns the current member of a given hierarchy at a given location.
//     *
//     * @param pos Coordinates in cell set
//     * @param hierarchy Hierarchy
//     * @return current member of given hierarchy
//     */
//    public Member getMember(int[] pos, Hierarchy hierarchy) {
//    	// Findbugs Issue - As per analysis, this is not a valid issue. No need to fix
//        for (int i = -1; i < axes.length; i++) {
//            Axis axis = slicerAxis;
//            int index = 0;
//            if (i >= 0) {
//                axis = axes[i];
//                index = pos[i];
//            }
//            List<Position> positions = axis.getPositions();
//            Position position = positions.get(index);
//            for (Member member : position) {
//                if (member.getHierarchy() == hierarchy) {
//                    return member;
//                }
//            }
//        }
//        return hierarchy.getHierarchy().getDefaultMember();
//    }
//	
//	class AbstractMTupleIterable extends AbstractTupleIterable{
//		
//		final MolapResultStream molapResultStream;
//		Iterator<MolapTuple> molapTupleIter;
//		MolapTuple currentMolapTuple;
//		
//		public AbstractMTupleIterable(MolapResultStream molapResultStream){
//			super(0);
//			this.molapResultStream = molapResultStream;
//			this.currentMolapTuple = null;
//			
//			if(molapResultStream.hasNext()){
//				MolapResultChunk molapResultChunk = molapResultStream.getResult();
//				molapTupleIter = molapResultChunk.getRowTuples().iterator();
//				if(molapTupleIter.hasNext()){
//					//currentMolapTuple = molapTupleIter.next();
//				}
//			}
//		}
//		
//		
//        public TupleCursor tupleCursor() {
//        	
//        	
//            return new AbstractTupleCursor(arity) {
//            	
//                public boolean forward() {
//                	if(molapTupleIter == null){
//                		return false;
//                	}
//                	if(!molapTupleIter.hasNext()){
//                		if(!molapResultStream.hasNext()){
//                			return false;
//                		}
//                		molapTupleIter = molapResultStream.getResult().getRowTuples().iterator();
//                		return forward();
//                	}
//                	currentMolapTuple = molapTupleIter.next();
//                	return true;
//                }
//
//                public List<Member> current() {
//                	List<Member> list = new ArrayList<Member>();
//                	RolapMember parentCubeMember = null;
//                	RolapMember currentCubeMember = null;
//                	
//                	for(Hierarchy hierarchy:resultMappinDTO.rowHierarchies){
//                		for(Level rolapLevel: hierarchy.getLevels()){
//                			Integer colPos = resultMappinDTO.levelToSQLColumnsMap.get(rolapLevel);
//                			if(colPos == null){
//                				continue;
//                			}
//                			currentCubeMember = createMember(hierarchy,(RolapLevel)rolapLevel,parentCubeMember
//                											,currentMolapTuple,colPos);
//                			if(currentCubeMember == null){
//                				continue;
//                			}
//                			parentCubeMember = currentCubeMember; 
//                		}
//                		list.add(parentCubeMember);
//                		parentCubeMember = null;
//                	}
//                
//                	return list;        
//                }
//            };
//        };
//
//	}
//	
//	class AbstractMColTupleIterable extends AbstractTupleIterable{
//		
//		final MolapResultStream molapResultStream;
//		Iterator<MolapTuple> molapTupleIter;
//		MolapTuple currentMolapTuple;
//		
//		public AbstractMColTupleIterable(MolapResultStream molapResultStream){
//			super(0);
//			this.molapResultStream = molapResultStream;
//			this.currentMolapTuple = null;
//			
//			molapTupleIter = molapResultStream.getColumnTuples().iterator();
//			if(molapTupleIter.hasNext()){
////				currentMolapTuple = molapTupleIter.next();
//			}
//		}
//		
//		
//        public TupleCursor tupleCursor() {
//        	
//        	
//            return new AbstractTupleCursor(arity) {
//            	
//                public boolean forward() {
//                	if(!molapTupleIter.hasNext()){
//                			return false;
//                		}
//                	currentMolapTuple = molapTupleIter.next();
//                	return true;
//                }
//
//                public List<Member> current() {
//                	List<Member> list = new ArrayList<Member>();
//                	RolapMember parentCubeMember = null;
//                	RolapMember currentCubeMember = null;
//                	
//                	for(Hierarchy hierarchy:resultMappinDTO.colHierarchies){
//                		for(Level rolapLevel: hierarchy.getLevels()){
//                			Integer colPos = resultMappinDTO.levelToSQLColumnsMap.get(rolapLevel);
//                			if(colPos == null){
//                				continue;
//                			}
//                			currentCubeMember = createMember(hierarchy,(RolapLevel)rolapLevel,parentCubeMember,currentMolapTuple,colPos);
//                			if(currentCubeMember == null){
//                				continue;
//                			}
//                			parentCubeMember = currentCubeMember; 
//                		}
//                		list.add(parentCubeMember);
//                		parentCubeMember = null;
//                	}
//                
//                	return list;                	
//                }
//
//				
//             
//            };
//        };
//
//	}
//	
//	private RolapMember createMember(Hierarchy hierarchy, RolapLevel rolapLevel, RolapMember parentCubeMember
//										, MolapTuple currentMolapTuple, Integer colPos) 
//	{
//		RolapMember resultRolapMember = null;
//		if(rolapLevel.isAll()){
//			return (RolapMember) hierarchy.getAllMember();
//		}
//		
//		MolapMember[] molapMembers = currentMolapTuple.getTuple();
//		if( molapMembers.length <= colPos ){
//			return null;
//		}
//		
//		MolapMember molapMember = molapMembers[colPos];
//		
//		if( !rolapLevel.isMeasure() ){ //if dimension
//			RolapMember parentMember = null;		
//			if(parentCubeMember != null){
//				parentMember = ((RolapCubeMember)parentCubeMember).getRolapMember();
//			}
//			RolapMember member = new RolapMemberBase(parentMember,rolapLevel,molapMember.getName());
//			resultRolapMember = new RolapCubeMember((RolapCubeMember)parentCubeMember, member, (RolapCubeLevel) rolapLevel);
//		
//		} else { //if measure
//			RolapMember parentMember = null;		
//			if(parentCubeMember != null){
//				parentMember = ((RolapCubeMember)parentCubeMember).getRolapMember();
//			}
//			RolapMember member = new RolapMemberBase(parentMember, rolapLevel,molapMember.getName());
//			resultRolapMember = member;
//		}
//		
//		return resultRolapMember;
//	}
//	
//	public RolapMember convertTupleToMember(MolapMember molapMember, RolapMember parentCubeMember 
//												,int pos, int axisNo, Set<Hierarchy> hierarchyAlreadyAdded){
//		RolapMember resultRolapMember = null;
//		boolean isMeasure = false;
//		
//		Hierarchy hierarchy = resultMappinDTO.colHierarchies.get(pos);
//		MolapLevel molapLevel = resultMappinDTO.selectedColLevels.get(pos);
//		
////		List<MolapLevelHolder> molapLevelHolderList = molapQueryImpl.getAxises()[axisNo].getDims();
////		
////		MolapLevelHolder molapLevelHolder = molapLevelHolderList.get(levelPos);
////		if( molapLevelHolder == null )
////		{   //return unknown
////			return null;
////		}
//		String levelName = "";
//		if(molapLevel.getType() == MolapLevel.MolapLevelType.DIMENSION)
//		{
//			levelName = molapLevel.getName();
//			
//		} else
//		{
//			levelName = "[Measures].[MeasuresLevel]";
//			isMeasure = true;
//		}
//		Level level;
//		try {
//			level = getHierarchyLevel(hierarchy, levelName);
//			hierarchy.getLevels();
//			
//		} catch (Exception e) {
//			level = null;
//		}
//	
//		
//		if(!isMeasure){
//
//			RolapMember parentMember = null;		
//			if(parentCubeMember != null){
//				parentMember = ((RolapCubeMember)parentCubeMember).getRolapMember();
//			}
//			RolapMember member = new RolapMemberBase(parentMember,(RolapLevel) level,molapMember.getName());
//			resultRolapMember = new RolapCubeMember((RolapCubeMember)parentCubeMember, member, (RolapCubeLevel) level);
//			
//		} else{
//			RolapMember parentMember = null;		
//			if(parentCubeMember != null){
//				parentMember = ((RolapCubeMember)parentCubeMember).getRolapMember();
//			}
//			RolapMember member = new RolapMemberBase(parentMember,(RolapLevel) level,molapMember.getName());
//			//resultRolapMember = new RolapBaseCubeMeasure((RolapCubeMember)parentCubeMember, member, (RolapCubeLevel) level);
//			resultRolapMember = member;
//		}
//		
//		return resultRolapMember;
//	}
//	
//	
//	public Level getHierarchyLevel(Cube cube, String level) throws Exception {
//        List<Id.Segment> segments = Util.parseIdentifier(level);
//        Hierarchy hierarchy = getHierarchy(cube, segments.get(0).toString());
//		Level[] levels = hierarchy.getLevels();
//		String levelName = MolapQueryParseUtil.getTokenAtIndex(level, 1);
//		levelName = MolapQueryParseUtil.stripBrackets(levelName);
//		for (Level levelObj : levels) {
//			if (levelObj.getName().equals(levelName))
//				return levelObj;
//		}
//        throw new Exception("Missing MetaData field : "+level);
//	}
//	
//	public Level getHierarchyLevel(Hierarchy hierarchy, String levelName) throws Exception {
//        Level[] levels = hierarchy.getLevels();
//		for (Level levelObj : levels) {
//			if (levelObj.getName().equals(levelName))
//				return levelObj;
//		}
//        throw new Exception("Missing MetaData field : "+levelName);
//	}
//	
//	
//	  /**
//		 * ClearView currently only supports one hierarchy per dimension.  Having multiple
//		 * hierarchies per dimension is confusing for the business the user
//		 * @param cube
//		 * @param hierarchyName
//		 * @return Hierarchy
//	 * @throws Exception 
//		 */
//		public static Hierarchy getHierarchy(Cube cube, String hierarchyName) {
//		    
//		    Hierarchy foundHierarchy = null;
//		    for (Dimension dimension : cube.getDimensions()) {
//	            Hierarchy[] hierarchies = dimension.getHierarchies();
//	            for (Hierarchy hierarchy : hierarchies) {
//	                String name = hierarchy.getName();
//	                if( ((RolapHierarchy)hierarchy).getSubName()!=null)
//	                {
//	                    name = ((RolapHierarchy)hierarchy).getSubName();
//	                }
//	                if (name.equals(hierarchyName)) {
//	                    foundHierarchy = hierarchy;
//	                    break;
//	                }
//	            }
//	        }
//		    
//	        return foundHierarchy;
//		}
//
//		public static class ResultMappinDTO
//		{
//	        /**
//	         * For each hierarchy present in query, till which level the Members should present in result
//	         */
//		    public Map<Hierarchy, Integer> hierarchyToLevelDepthMap = new HashMap<Hierarchy, Integer>();
//		    
//		    /**
//		     * List of hierarchies on rows to represent in result to analyser
//		     */
//		    public List<Hierarchy> rowHierarchies = new ArrayList<Hierarchy>();
//	        
//	        /**
//	         *  List of hierarchies on columns to represent in result to analyser
//	         */
//		    public List<Hierarchy> colHierarchies = new ArrayList<Hierarchy>();
//		    
//		    
//	        /**
//	         * Level/Measure name to SQL result column index
//	         */
//		    public Map<Level, Integer> levelToSQLColumnsMap = new HashMap<Level, Integer>();
//	        
//	        /**
//	         *  List of Levels present on rows axis
//	         */
//		    public List<MolapLevel> selectedRowLevels = new ArrayList<MolapLevel>();
//	        
//	        /**
//	         * List of Levels present on columns axis
//	         */
//		    public List<MolapLevel> selectedColLevels = new ArrayList<MolapLevel>();
//	        
//	        /**
//	         * List of measures defined in query
//	         */
//		    public List<MolapLevel> selectedMeasures = new ArrayList<MolapLevel>();
//		    
//		    /**
//             * Dynamic Levels created as part of query which are not defined in base cube.
//             */
//            public  Map<String, Level> dynamicLevels = new HashMap<String, Level>();
//		    
//
//            /**
//             * SQL representation of query 
//             */
//            public String sqlQuery;
//		}
//
//		@Override
//		public int getTotalRowCount() 
//		{
//			// TODO Auto-generated method stub
//			return 0;
//		}
}

