import java.util
import java.util.Locale.ENGLISH
import java.util.Optional

import com.facebook.presto.Session
import com.facebook.presto.execution.QueryIdGenerator
import com.facebook.presto.hive.HiveHadoop2Plugin
import com.facebook.presto.metadata.SessionPropertyManager
import com.facebook.presto.spi.`type`.TimeZoneKey.UTC_KEY
import com.facebook.presto.spi.security.Identity
import com.facebook.presto.tests.DistributedQueryRunner
import com.google.common.collect.ImmutableMap
import org.slf4j.LoggerFactory

object PrestoHiveServerRunner {

  val log = LoggerFactory.getLogger(getClass)

  val HIVE_CATALOG = "hive"
  val HIVE_CONNECTOR = "hive-hadoop2"
  val HIVE_SOURCE = "hive"
  val HIVE_SCHEMA = "benchmarking"
  val HIVE_METASTORE_URI="thrift://localhost:9083"

  @throws[Exception]
  def createQueryRunner(extraProperties: util.Map[String, String]): DistributedQueryRunner = {
    val queryRunner = new DistributedQueryRunner(createSession, 4, extraProperties)
    try {
      queryRunner.installPlugin(new HiveHadoop2Plugin())
      val hiveProperties = ImmutableMap.builder[String, String]
        .put("hive.metastore.uri",HIVE_METASTORE_URI ).build

      queryRunner.createCatalog(HIVE_CATALOG, HIVE_CONNECTOR, hiveProperties)
      queryRunner
    } catch {
      case e: Exception =>
        queryRunner.close()
        throw e
    }
  }

  def createSession: Session = Session.builder(new SessionPropertyManager).setQueryId(new QueryIdGenerator().createNextQueryId).setIdentity(new Identity("user", Optional.empty())).setSource(HIVE_SOURCE).setCatalog(HIVE_CATALOG).setSchema(HIVE_SCHEMA).setTimeZoneKey(UTC_KEY).setLocale(ENGLISH).setRemoteUserAddress("address").setUserAgent("agent").build
}


