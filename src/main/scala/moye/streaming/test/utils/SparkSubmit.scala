package moye.streaming.test.utils

/**
 * Created by admin on 7/23/15.
 */

import org.apache.spark.Logging

object SparkSubmit extends Logging {
  def main(args: Array[String]) {
    com.aliyun.odps.spark.SparkSubmit.main(args)
  }
}

