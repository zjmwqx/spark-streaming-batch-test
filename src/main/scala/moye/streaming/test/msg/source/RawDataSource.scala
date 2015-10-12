package moye.streaming.test.msg.source

import scala.util.Random

/**
 * Created by admin on 9/21/15.
 */
class RawDataSource(size :Int) extends DataSource[String] {
  val strings = Array((1 to 1024 * size).map(x => "words").mkString,(1 to 1024 * size).map(x =>  "may  ").mkString, (1 to 1024 * size).map(x => "count").mkString)
//    val strings = Array( "words", "may  ", "count")
//  val bt = (1 to 1024).toArray

  def getMassage(): String= {
    val rand = new Random()
    val x = rand.nextInt(3)
    strings(x)
//    bt
  }
}
