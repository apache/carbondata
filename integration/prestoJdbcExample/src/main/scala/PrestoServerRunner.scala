import java.util
import java.util.Locale.ENGLISH
import java.util.Optional

import com.facebook.presto.Session
import com.facebook.presto.execution.QueryIdGenerator
import com.facebook.presto.metadata.SessionPropertyManager
import com.facebook.presto.spi.`type`.TimeZoneKey.UTC_KEY
import com.facebook.presto.spi.security.Identity
import com.facebook.presto.tests.DistributedQueryRunner
import com.google.common.collect.ImmutableMap
import org.apache.carbondata.presto.CarbondataPlugin

object PrestoServerRunner {

  /**
    * CARBONDATA_CATALOG : stores the name of the catalog in the etc/catalog for Apache CarbonData.
    * CARBONDATA_CONNECTOR : stores the name of the CarbonData connector for Presto.
    * The etc/catalog will contain a catalog file for CarbonData with name <CARBONDATA_CATALOG>.properties
    * and following content :
    * connector.name = <CARBONDATA_CONNECTOR>
    * carbondata-store = <CARBONDATA_STOREPATH>
    */
  val CARBONDATA_CATALOG = "carbondata"
  val CARBONDATA_CONNECTOR = "carbondata"
  val CARBONDATA_STOREPATH = "hdfs://localhost:54310/user/hive/warehouse/carbon.store"
  val CARBONDATA_SOURCE = "carbondata"

  /**
    * Instantiates the Presto Server to connect with the Apache CarbonData
    *
    * @param extraProperties
    * @return Instance of running server.
    * @throws Exception
    */
  @throws[Exception]
  def createQueryRunner(extraProperties: util.Map[String, String]): DistributedQueryRunner = {
    val queryRunner = new DistributedQueryRunner(createSession, 4, extraProperties)
    try {
      queryRunner.installPlugin(new CarbondataPlugin)
      val carbonProperties = ImmutableMap.builder[String, String].put("carbondata-store", CARBONDATA_STOREPATH).build
      /**
        * createCatalog will create a catalog for CarbonData in etc/catalog. It takes following parameters
        * CARBONDATA_CATALOG : catalog name
        * CARBONDATA_CONNECTOR : connector name
        * carbonProperties : Map of properties to be configured for CarbonData Connector.
        */
      queryRunner.createCatalog(CARBONDATA_CATALOG, CARBONDATA_CONNECTOR, carbonProperties)
      queryRunner
    } catch {
      case e: Exception =>
        queryRunner.close()
        throw e
    }
  }

  /**
    * createSession will create a new session in the Server to connect and execute queries.
    *
    * @return a Session instance
    */
  def createSession: Session = Session.builder(new SessionPropertyManager).setQueryId(new QueryIdGenerator().createNextQueryId).setIdentity(new Identity("user", Optional.empty())).setSource(CARBONDATA_SOURCE).setCatalog(CARBONDATA_CATALOG).setTimeZoneKey(UTC_KEY).setLocale(ENGLISH).setRemoteUserAddress("address").setUserAgent("agent").build

}
