package moye.streaming.test.msg.source

import scala.reflect.ClassTag

/**
 * Created by admin on 9/21/15.
 */
abstract class DataSource[T:ClassTag] {
  def preStart():Unit = {}
  def getMassage():T
}
