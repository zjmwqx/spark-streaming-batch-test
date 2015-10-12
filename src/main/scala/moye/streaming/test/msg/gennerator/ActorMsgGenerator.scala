package moye.streaming.test.msg.gennerator

import akka.actor.{Props, ActorRef, Actor}
import moye.streaming.test.msg.source.{RawDataSource, DataSource}
import moye.streaming.test.utils.{Utils, AkkaUtils}
import org.apache.spark.SparkConf

import scala.collection.mutable
import scala.collection.mutable.LinkedList
import scala.reflect.ClassTag
import scala.util.Random

/**
 * Created by admin on 9/17/15.
 */
case class SubscribeReceiver(receiverActor: ActorRef)

class ActorMsgGenerator[T: ClassTag](source: DataSource[T]) extends Actor {
  var receivers: LinkedList[ActorRef] = new LinkedList[ActorRef]()

  def receive: Receive = {
    case SubscribeReceiver(receiverActor: ActorRef) =>

    case UnsubscribeReceiver(receiverActor: ActorRef) =>
  }

  val strings: Array[String] = Array("words ", "may ", "count ")
  val rand = new Random()
  //for push

  new Thread() {
    override def run() {
      try {
        source.preStart()
        while (true) {
          Thread.sleep(5)
          receivers.foreach(_ ! source.getMassage())
        }
      }catch{
        case e:Exception => e.printStackTrace()
      }
    }
  }.start()
}

case class UnsubscribeReceiver(receiverActor: ActorRef)
case class GetMsg(receiverActor: ActorRef)
case class RegisterOK(receiverActor: ActorRef)
case class HostPort(host:String, port :Int)
object testActor {
  def main(args: Array[String]) {
    val conf = new SparkConf
    val actorSystem = AkkaUtils.createActorSystem("test", Utils.findLocalInetAddress.getHostAddress, "9899".toInt, conf = conf)._1
    val generator = actorSystem.actorOf(Props(new ActorMsgGenerator(new RawDataSource(1))), "msgGeneratorActor")

    println("ActorMsgGenerator started as:" + generator)

    actorSystem.awaitTermination()
  }
}
