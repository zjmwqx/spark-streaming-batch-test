//package moye.streaming.test.msg
//
//import akka.actor.{ActorRef, Actor, ExtendedActorSystem, Props}
//import moye.streaming.test.msg.gennerator.ActorMsgGenerator
//import moye.streaming.test.msg.receiver.ActorMsgReceiver
//import moye.streaming.test.msg.source.{RawDataSource, DataSource}
//import moye.streaming.test.utils.{AkkaUtils, Utils}
//import org.apache.spark.storage.StorageLevel
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.apache.spark.{Logging, SparkContext, SparkConf}
//
//import scala.reflect.ClassTag
//
///**
// * Created by admin on 9/17/15.
// */
//class MsgDispatcher[T: ClassTag](source: DataSource[T]) extends Serializable {
//  val name = "MsgGenerator"
//  val actorName = "msgGeneratorActor"
//  var driverHostUrl: String = _
//  var port: Int = _
//
//  def start(): Unit = {
//    val conf = new SparkConf
//    val localhost = Utils.findLocalInetAddress.getHostAddress
//    val actorSystem = AkkaUtils.createActorSystem(name, localhost,
//      port, conf = conf)._1
//    val driverRef = actorSystem.actorSelection(driverHostUrl)
//    val generator = actorSystem.actorOf(Props(new ActorMsgGenerator(source)), actorName)
//    val localPort = actorSystem.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress.port.get
//    driverRef ! s"akka.tcp://$name@$localhost:$localPort/user/$actorName"
//    //    driverRef ! generator
//    println(s"akka.tcp://$name@$localhost:$localPort/user/$actorName")
//    println("ActorMsgGenerator started as:" + generator)
//    actorSystem.awaitTermination()
//  }
//}
//
//class DriverActor extends Actor {
//  def receive: Receive = {
//    case msg: String => {
//      println(msg)
//      MsgDisPatcher.dispatchers += msg
//    }
//  }
//}
//
//object MsgDisPatcher extends Logging {
//  @volatile var dispatchers = scala.collection.mutable.Set[String]()
//
//
//  def pipelineTest[T: ClassTag](sourceList: Array[MsgDispatcher[T]]): Unit = {
//
//
//    def startDisp(msgDisp: Iterator[MsgDispatcher[T]]): Unit = {
//      for (disp <- msgDisp) disp.start()
//    }
//
//    //initialize driver and dispatcher meta
//    val name = "Driver"
//    val actorName = "driverActor"
//    val port = 8088
//    val localhost = Utils.findLocalInetAddress.getHostAddress
//    val conf = new SparkConf(true)
//    val actorSystem = AkkaUtils.createActorSystem(name,
//      localhost, port, conf = conf)._1
//    val driver = actorSystem.actorOf(Props[DriverActor], actorName)
//    println("driver started as:" + driver)
//    val localPort = actorSystem.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress.port.get
//    var cnt = 0
//    for (source <- sourceList) {
//      source.driverHostUrl = s"akka.tcp://$name@$localhost:$localPort/user/$actorName"
//      source.port = port + cnt
//      cnt += 1
//    }
//
//    //dispatch msg generators
//    val sc = new SparkContext(conf)
//    //start stream sources
//    new Thread() {
//      override def run(): Unit = {
//        try {
//          val rdd = sc.makeRDD(sourceList, sourceList.size)
//          sc.makeRDD(1 to 50, 50).map(x => (x, 1)).reduceByKey(_ + _, 20).collect()
//          sc.runJob(rdd, startDisp _)
//        } catch {
//          case e: Exception => e.printStackTrace()
//        }
//      }
//    }.start
//    //start streaming actor receiver job
//    println("can go here")
//    while (dispatchers.size < sourceList.size) {
//      println(dispatchers.size + " " + sourceList.size)
//      Thread.sleep(1000)
//    }
//    println("all dispatcher has started!")
//    new Thread() {
//      override def run(): Unit = {
//        try {
//          val ssc = new StreamingContext(sc, Seconds(10))
////          ssc.checkpoint("")
//          val streams = ssc.union((
//            for (dispatcherRefString <- dispatchers) yield
//              //here we should set String. if not set will cause wrong
//              ssc.actorStream[String](Props(new ActorMsgReceiver[String](dispatcherRefString)), "actorReceiver",
//                StorageLevel.MEMORY_AND_DISK_SER)
//            ).toSeq
//          )
//          streams.count.print()
//          ssc.start
//          println("ssc has started")
//          ssc.awaitTermination()
//        } catch {
//          case e: Exception => e.printStackTrace()
//        }
//      }
//    }.start
//    actorSystem.awaitTermination()
//  }
//
//  def main(args: Array[String]): Unit = {
//    //    val odpsUrl = "http://10.101.207.200:8375/api"
//    //    val accessId = "63wd3dpztlmb5ocdkj94pxmm"
//    //    val accessKey = "oRd30z7sV4hBX9aYtJgii5qnyhg="
//    //    val tunnelUrl = "http://10.101.207.200:8095"
//    //    val project = "odps_moye_test"
//    //    val table = "datahub_test_table_failover"
//    //    val parallism = 1
//    //    val subscribe = DatahubUtils.initSubscription(odpsUrl, accessId, accessKey, tunnelUrl, project, table, 1, 10, 10)
//    //    val msgDispatchers = (1 to 10).map(i => new MsgDispatcher(new DatahubSource(
//    //      odpsUrl, accessId, accessKey, tunnelUrl, project, table, parallism, subscribe
//    //    )))
//    //    MsgDisPatcher.pipelineTest[Array[String]](msgDispatchers.toArray)
//    val msgDispatchers = (1 to 4).map(i => new MsgDispatcher(new RawDataSource(args(0).toInt)))
//    MsgDisPatcher.pipelineTest[String](msgDispatchers.toArray)
//  }
//}
