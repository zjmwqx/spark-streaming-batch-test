package moye.streaming.test.msg.receiver

import akka.actor.{ActorSystem, Props, ActorRef, Actor}
import com.aliyun.odps.data.Record
import moye.streaming.test.msg.gennerator.{GetMsg, RegisterOK, UnsubscribeReceiver, SubscribeReceiver}
import moye.streaming.test.utils.AkkaUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.receiver.ActorHelper

import scala.reflect.ClassTag

/**
 * Created by admin on 9/17/15.
 */

class ActorMsgReceiver[T: ClassTag](urlOfPublisher: String) extends Actor with ActorHelper {
  lazy private val remotePublisher = context.actorSelection(urlOfPublisher)
  @volatile var registered = false
  override def preStart(): Unit = {
    remotePublisher ! SubscribeReceiver(context.self)
    //for get
  }

  def receive: PartialFunction[Any, Unit] = {
    //    case arrayMsg:Array[T] => store(arrayMsg.iterator)
    case RegisterOK(receiverActor: ActorRef) => {
      registered = true
    }
    case msg => {
      store(msg)
    }
  }

  new Thread() {
    override def run(): Unit = {
      try {
        while(true) {
          if(registered)
            remotePublisher
        }
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
  }.start

  override def postStop(): Unit = remotePublisher ! UnsubscribeReceiver(context.self)
}

object ActorMsgReceiver {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val actorSystem = AkkaUtils.createActorSystem("test", "10.101.203.71", "9898".toInt, conf = conf)._1
    val sys = actorSystem.actorOf(Props(new
        ActorMsgReceiver("akka.tcp://Driver@10.101.203.71:8089/user/Driver")), "receiver")
    actorSystem.awaitTermination()
  }
}

