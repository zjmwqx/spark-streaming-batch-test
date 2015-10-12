package moye.streaming.test.msg

import java.io.{IOException, ByteArrayOutputStream}
import java.net.ServerSocket
import java.nio.ByteBuffer

import akka.actor.{ActorRef, Actor, ExtendedActorSystem, Props}
import moye.streaming.test.msg.gennerator.{HostPort, ActorMsgGenerator}
import moye.streaming.test.msg.receiver.ActorMsgReceiver
import moye.streaming.test.msg.source.{RawDataSource, DataSource}
import moye.streaming.test.utils.{RateLimitedOutputStream, FileMoke, AkkaUtils, Utils}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{Logging, SparkContext, SparkConf}

import scala.reflect.ClassTag


/**
 * Created by admin on 9/17/15.
 */
class RawDispatcher[T: ClassTag](_bytesPerSec: Int) extends Serializable {
  val name = "MsgGenerator"
  val actorName = "msgGeneratorActor"
  var driverHostUrl: String = _
  var _port: Int = 8888

  def start(): Unit = {
    val _conf = new SparkConf
    val localhost = Utils.findLocalInetAddress.getHostAddress
    val actorSystem = AkkaUtils.createActorSystem(name, localhost,
      _port, conf = _conf)._1
    val driverRef = actorSystem.actorSelection(driverHostUrl)


    var (port, bytesPerSec) = (_port + 1, _bytesPerSec)
    val blockSize = bytesPerSec / 10
    // Repeat the input data multiple times to fill in a buffer
    val lines = FileMoke.file.split("\n", -1).toArray
    println(lines.size)
    val bufferStream = new ByteArrayOutputStream(blockSize + 1000)
    val ser = new KryoSerializer(new SparkConf()).newInstance()
    val serStream = ser.serializeStream(bufferStream)
    var i = 0
    while (bufferStream.size < blockSize) {
      serStream.writeObject(lines(i))
      i = (i + 1) % lines.length
    }
    val array = bufferStream.toByteArray

    val countBuf = ByteBuffer.wrap(new Array[Byte](4))
    countBuf.putInt(array.length)
    countBuf.flip()
    var serverSocket: ServerSocket = null
    var succeed = false
    while (!succeed)
      try {
        println(port)
        port += 1
        serverSocket = new ServerSocket(port)
        succeed = true
      } catch {
        case e: Exception => e.printStackTrace()
      }
    println("Listening ocntn port " + port)

    driverRef ! HostPort(localhost, port)

    while (true) {
      val socket = serverSocket.accept()
      println("Got a new connection")
      val out = new RateLimitedOutputStream(socket.getOutputStream, bytesPerSec)
      try {
        while (true) {
          out.write(countBuf.array)
          out.write(array)
        }
      } catch {
        case e: IOException =>
          println("Client disconnected")
          socket.close()
      }
    }

    actorSystem.awaitTermination()
  }
}

class DriverActor extends Actor {
  def receive: Receive = {
    case msg: HostPort => {
      println(msg)
      RawDispatcher.dispatchers += msg
    }
  }
}

object RawDispatcher extends Logging {
  @volatile var dispatchers = scala.collection.mutable.Set[HostPort]()


  def pipelineTest[T: ClassTag](sourceList: Array[RawDispatcher[T]]): Unit = {


    def startDisp(msgDisp: Iterator[RawDispatcher[T]]): Unit = {
      for (disp <- msgDisp) disp.start()
    }

    //initialize driver and dispatcher meta
    val name = "Driver"
    val actorName = "driverActor"
    val port = 8088
    val localhost = Utils.findLocalInetAddress.getHostAddress
    val conf = new SparkConf(true)
    val actorSystem = AkkaUtils.createActorSystem(name,
      localhost, port, conf = conf)._1
    val driver = actorSystem.actorOf(Props[DriverActor], actorName)
    println("driver started as:" + driver)
    val localPort = actorSystem.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress.port.get
    var cnt = 0
    for (source <- sourceList) {
      source.driverHostUrl = s"akka.tcp://$name@$localhost:$localPort/user/$actorName"
      source._port = port + cnt
      cnt += 1
    }

    //dispatch msg generators
    val sc = new SparkContext(conf)
    //start stream sources
    new Thread() {
      override def run(): Unit = {
        try {
          val rdd = sc.makeRDD(sourceList, sourceList.size)
          sc.makeRDD(1 to 50, 50).map(x => (x, 1)).reduceByKey(_ + _, 20).collect()
          sc.runJob(rdd, startDisp _)
        } catch {
          case e: Exception => e.printStackTrace()
        }
      }
    }.start
    //start streaming actor receiver job
    println("can go here")
    while (dispatchers.size < sourceList.size) {
      println(dispatchers.size + " " + sourceList.size)
      Thread.sleep(1000)
    }
    println("all dispatcher has started!")
    new Thread() {
      override def run(): Unit = {
        try {
          val ssc = new StreamingContext(sc, Seconds(10))
//          ssc.checkpoint("")
          val rawStreams = dispatchers.map(hostPort =>
            ssc.rawSocketStream[String](hostPort.host, hostPort.port, StorageLevel.MEMORY_ONLY_SER)).toArray
          val union = ssc.union(rawStreams)
          union.count().map(c => s"Received $c records").print()
          ssc.start
          println("ssc has started")
          ssc.awaitTermination()
        } catch {
          case e: Exception => e.printStackTrace()
        }
      }
    }.start
    actorSystem.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    //    val odpsUrl = "http://10.101.207.200:8375/api"
    //    val accessId = "63wd3dpztlmb5ocdkj94pxmm"
    //    val accessKey = "oRd30z7sV4hBX9aYtJgii5qnyhg="
    //    val tunnelUrl = "http://10.101.207.200:8095"
    //    val project = "odps_moye_test"
    //    val table = "datahub_test_table_failover"
    //    val parallism = 1
    //    val subscribe = DatahubUtils.initSubscription(odpsUrl, accessId, accessKey, tunnelUrl, project, table, 1, 10, 10)
    //    val msgDispatchers = (1 to 10).map(i => new MsgDispatcher(new DatahubSource(
    //      odpsUrl, accessId, accessKey, tunnelUrl, project, table, parallism, subscribe
    //    )))
    //    MsgDisPatcher.pipelineTest[Array[String]](msgDispatchers.toArray)
    val rawDispatcher = (1 to args(0).toInt).map(i => new RawDispatcher[String](args(1).toInt))
    RawDispatcher.pipelineTest[String](rawDispatcher.toArray)
  }
}
