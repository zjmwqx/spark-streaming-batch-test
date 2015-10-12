package moye.streaming.test.msg.source

import java.io.IOException
import java.util.concurrent.{Callable, Executors, ExecutorCompletionService}

import com.aliyun.odps.Odps
import com.aliyun.odps.account.AliyunAccount
import com.aliyun.odps.data.Record
import com.aliyun.odps.datahub.{Subscription, RecordPack, Consumer, SubscriptionConstant}
import com.aliyun.odps.tunnel.{TunnelException, SubscriptionClient, TableTunnel}

import scala.reflect.ClassTag

/**
 * Created by admin on 9/21/15.
 */
class DatahubSource(
                     odpsUrl: String,
                     accessId: String,
                     accessKey: String,
                     tunnelUrl: String,
                     project: String,
                     table: String,
                     parallism: Int,
                     subscribe: String
                     ) extends DataSource[Array[String]] {

  @transient var reader: Consumer = null

  override def preStart(): Unit = {
    val account = new AliyunAccount(accessId, accessKey)
    val odps = new Odps(account)
    odps.setDefaultProject(project)
    odps.setEndpoint(odpsUrl)
    try {
      val client = new SubscriptionClient(odps)
      client.setTunnelEndpoint(tunnelUrl)
      println(s"subkey: ${subscribe}")
      val subscription = client.loadSubscription(project, table, subscribe)
      reader = subscription.createConsumer()
      println("reader creaded")
    }
  }

  import scala.collection.JavaConverters._

  override def getMassage(): Array[String] = {
    var records: Array[Record] = null
    try {
      records = reader.readRecordPack().getRecords.asScala.toArray
    }
    catch {
      case e: TunnelException => e.printStackTrace()
      case s: IOException => s.printStackTrace()
    }
    records.map(_.getString(4))
  }
}



