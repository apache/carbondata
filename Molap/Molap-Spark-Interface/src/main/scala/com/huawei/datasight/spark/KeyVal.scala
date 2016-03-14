
/**
 * It is just Key value class. I don't get any other alternate to make the RDD class to work with my minimum knowledge in scala.
 * May be I will remove later once I gain good knowledge :)
  *
 * @author R00900208
 *
 */

package com.huawei.datasight.spark

import com.huawei.unibi.molap.engine.scanner.impl.MolapKey
import com.huawei.unibi.molap.engine.scanner.impl.MolapValue
import com.huawei.datasight.molap.core.load.LoadMetadataDetails
import com.huawei.datasight.molap.load.DeletedLoadMetadata


trait KeyVal[K, V] extends Serializable {
  def getKey(key: MolapKey, value: MolapValue): (K, V)

}

class KeyValImpl extends KeyVal[MolapKey, MolapValue] {
  override def getKey(key: MolapKey, value: MolapValue) = (key, value)
}

trait Result[K, V] extends Serializable {
  def getKey(key: Int, value: LoadMetadataDetails): (K, V)

}

class ResultImpl extends Result[Int, LoadMetadataDetails] {
  override def getKey(key: Int, value: LoadMetadataDetails) = (key, value)
}


trait PartitionResult[K, V] extends Serializable {
  def getKey(key: Int, value: Boolean): (K, V)

}

class PartitionResultImpl extends PartitionResult[Int, Boolean] {
  override def getKey(key: Int, value: Boolean) = (key, value)
}

trait MergeResult[K,V] extends Serializable
{
  def getKey(key : Int,value : Boolean ) : (K,V) 
    
}

class MergeResultImpl extends MergeResult[Int,Boolean]
{
  override def getKey(key : Int,value : Boolean) = (key,value)
}

trait DeletedLoadResult[K,V] extends Serializable
{
  def getKey(key : String,value : String) : (K,V) 
}

class DeletedLoadResultImpl extends DeletedLoadResult[String, String] {
  override def getKey(key: String, value: String) = (key, value)
}

trait RestructureResult[K, V] extends Serializable {
  def getKey(key: Int, value: Boolean): (K, V)
}

class RestructureResultImpl extends RestructureResult[Int, Boolean] {
  override def getKey(key: Int, value: Boolean) = (key, value)
}
