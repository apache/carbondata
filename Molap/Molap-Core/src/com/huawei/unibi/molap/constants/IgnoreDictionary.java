/**
 * 
 */
package com.huawei.unibi.molap.constants;

/**
 * This enum is used for determining the indexes of the
 * dimension,ignoreDictionary,measure columns.
 * 
 * @author R00903928
 * 
 */
public enum IgnoreDictionary {
    /**
     * POSITION WHERE DIMENSIONS R STORED IN OBJECT ARRAY.
     */
   DIMENSION_INDEX_IN_ROW (0),
    
   /**
    * POSITION WHERE BYTE[] (high cardinality) IS STORED IN OBJECT ARRAY.
    */
   BYTE_ARRAY_INDEX_IN_ROW(1),
   
    /**
     * POSITION WHERE MEASURES R STORED IN OBJECT ARRAY.
     */
    MEASURES_INDEX_IN_ROW (2);
    
   
   private final int index;

   IgnoreDictionary(int index) {
       this.index = index;
   }
   
   public int getIndex() {
       return this.index;
   }

}
