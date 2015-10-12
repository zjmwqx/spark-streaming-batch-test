package com.aliyun.spark.streaming

/**
 * Created by admin on 9/29/15.
 */
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.util.Random
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import scala.util.Random
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions

class CustomReceiver extends Receiver[String](StorageLevel.MEMORY_AND_DISK) {

  var stop = false

  override def onStart() {
    new Thread() {
      override def run(){
        receive(10000000)
      }
    }.start();
  }

  override def onStop() {
    this.stop = true
  }

  private def receive(num: Int) {
    val r = new Random()
    while (!stop) {
      val startTime = java.lang.System.currentTimeMillis()
      for (i <- 1 to num) {
        this.store(CustomReceiver.data(r.nextInt(CustomReceiver.data.length)))
      }
      println("stored 100 to rdd memory")
      val endTime = java.lang.System.currentTimeMillis()
      val sleep = endTime - startTime
      println(s"take $sleep s to store")
      if (sleep < 1000) {
        Thread sleep 1000 - sleep
      }
    }
  }
}

object CustomReceiver {
  val data = ('a' to 'z').map(_.toString * 1000).toArray
}

object StreamingWAL {
  def main(args: Array[String]) {
    val receiveCount = if (args.length > 0) args(0) else "10000"
    val conf = new SparkConf(true)
    conf.set("spark.executor.instances", "10")
    conf.set("spark.task.cpus", "200")
    conf.set("spark.executor.memory", "2048")
    conf.set("spark.cleaner.ttl.job.status", "20")
    conf.set("spark.streaming.receiver.writeAheadLog.enable", "true")
    conf.set("spark.streaming.blockInterval","1000ms")
    conf.set("spark.streaming.receiver.blockStoreTimeout","300")
    conf.set("spark.streaming.receiver.maxRate",receiveCount)


    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(1))
    ssc.checkpoint("")
    val lines = ssc.receiverStream(new CustomReceiver())

    val words = lines.filter(_ != "").map(word => {
      assert(CustomReceiver.data.contains(word))
      (word, 1L)
    }).reduceByKeyAndWindow((v1:Long,v2:Long) => (v1+v2),Seconds(1),Seconds(1))
      .map(kv => {("total_count",kv._2)}).reduceByKey(_+_).print


    ssc.start()
    Thread sleep 300*1000
    ssc.stop()
    ssc.awaitTermination()
  }
}