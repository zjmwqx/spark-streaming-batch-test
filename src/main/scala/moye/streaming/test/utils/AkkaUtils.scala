package moye.streaming.test.utils

/**
 * Created by admin on 9/17/15.
 */

import scala.collection.JavaConversions.mapAsJavaMap
import scala.concurrent.Await
import scala.concurrent.duration.FiniteDuration

import akka.actor.{ActorRef, ActorSystem, ExtendedActorSystem}
import akka.pattern.ask

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}

import org.apache.spark.{Logging, SparkConf, SparkEnv, SparkException}

/**
 * Various utility classes for working with Akka.
 */
object AkkaUtils extends Logging {

  /**
   * Creates an ActorSystem ready for remoting, with various Spark features. Returns both the
   * ActorSystem itself and its port (which is hard to get from Akka).
   *
   * Note: the `name` parameter is important, as even if a client sends a message to right
   * host + port, if the system name is incorrect, Akka will drop the message.
   *
   * If indestructible is set to true, the Actor System will continue running in the event
   * of a fatal exception. This is used by [[org.apache.spark.executor.Executor]].
   */
  def createActorSystem(
                         name: String,
                         host: String,
                         port: Int,
                         conf: SparkConf): (ActorSystem, Int) = {
    val startService: Int => (ActorSystem, Int) = { actualPort =>
      doCreateActorSystem(name, host, actualPort, conf)
    }
    Utils.startServiceOnPort(port, startService, conf, name)
  }

  private def doCreateActorSystem(
                                   name: String,
                                   host: String,
                                   port: Int,
                                   conf: SparkConf): (ActorSystem, Int) = {

    val akkaThreads = conf.getInt("spark.akka.threads", 4)
    val akkaBatchSize = conf.getInt("spark.akka.batchSize", 15)
    val akkaTimeoutS = conf.getTimeAsSeconds("spark.akka.timeout",
      conf.get("spark.network.timeout", "120s"))
    val akkaFrameSize = maxFrameSizeBytes(conf)
    val akkaLogLifecycleEvents = conf.getBoolean("spark.akka.logLifecycleEvents", false)
    val lifecycleEvents = if (akkaLogLifecycleEvents) "on" else "off"
    if (!akkaLogLifecycleEvents) {
      // As a workaround for Akka issue #3787, we coerce the "EndpointWriter" log to be silent.
      // See: https://www.assembla.com/spaces/akka/tickets/3787#/
      Option(Logger.getLogger("akka.remote.EndpointWriter")).map(l => l.setLevel(Level.FATAL))
    }

    val logAkkaConfig = if (conf.getBoolean("spark.akka.logAkkaConfig", false)) "on" else "off"

    val akkaHeartBeatPausesS = conf.getTimeAsSeconds("spark.akka.heartbeat.pauses", "6000s")
    val akkaHeartBeatIntervalS = conf.getTimeAsSeconds("spark.akka.heartbeat.interval", "1000s")


    val akkaConf = ConfigFactory.parseMap(conf.getAkkaConf.toMap[String, String])
      .withFallback(ConfigFactory.parseString(
      s"""
      |akka.daemonic = on
      |akka.loggers = [""akka.event.slf4j.Slf4jLogger""]
      |akka.stdout-loglevel = "ERROR"
      |akka.jvm-exit-on-fatal-error = off
      |akka.remote.transport-failure-detector.heartbeat-interval = $akkaHeartBeatIntervalS s
      |akka.remote.transport-failure-detector.acceptable-heartbeat-pause = $akkaHeartBeatPausesS s
      |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
      |akka.remote.netty.tcp.transport-class = "akka.remote.transport.netty.NettyTransport"
      |akka.remote.netty.tcp.hostname = "$host"
      |akka.remote.netty.tcp.port = $port
      |akka.remote.netty.tcp.tcp-nodelay = on
      |akka.remote.netty.tcp.connection-timeout = $akkaTimeoutS s
      |akka.remote.netty.tcp.maximum-frame-size = ${akkaFrameSize}B
      |akka.remote.netty.tcp.execution-pool-size = $akkaThreads
      |akka.actor.default-dispatcher.throughput = $akkaBatchSize
      |akka.log-config-on-start = $logAkkaConfig
      |akka.remote.log-remote-lifecycle-events = $lifecycleEvents
      |akka.log-dead-letters = $lifecycleEvents
      |akka.log-dead-letters-during-shutdown = $lifecycleEvents
      """.stripMargin))

    val actorSystem = ActorSystem(name, akkaConf)
    val provider = actorSystem.asInstanceOf[ExtendedActorSystem].provider
    val boundPort = provider.getDefaultAddress.port.get
    (actorSystem, boundPort)
  }

  private val AKKA_MAX_FRAME_SIZE_IN_MB = Int.MaxValue / 1024 / 1024

  /** Returns the configured max frame size for Akka messages in bytes. */
  def maxFrameSizeBytes(conf: SparkConf): Int = {
    val frameSizeInMB = conf.getInt("spark.akka.frameSize", 10)
    if (frameSizeInMB > AKKA_MAX_FRAME_SIZE_IN_MB) {
      throw new IllegalArgumentException(
        s"spark.akka.frameSize should not be greater than $AKKA_MAX_FRAME_SIZE_IN_MB MB")
    }
    frameSizeInMB * 1024 * 1024
  }

  /** Space reserved for extra data in an Akka message besides serialized task or task result. */
  val reservedSizeBytes = 200 * 1024

  /**
   * Send a message to the given actor and get its result within a default timeout, or
   * throw a SparkException if this fails.
   */
  def askWithReply[T](
                       message: Any,
                       actor: ActorRef,
                       timeout: FiniteDuration): T = {
    askWithReply[T](message, actor, maxAttempts = 1, retryInterval = Int.MaxValue, timeout)
  }

  /**
   * Send a message to the given actor and get its result within a default timeout, or
   * throw a SparkException if this fails even after the specified number of retries.
   */
  def askWithReply[T](
                       message: Any,
                       actor: ActorRef,
                       maxAttempts: Int,
                       retryInterval: Long,
                       timeout: FiniteDuration): T = {
    // TODO: Consider removing multiple attempts
    if (actor == null) {
      throw new SparkException(s"Error sending message [message = $message]" +
        " as actor is null ")
    }
    var attempts = 0
    var lastException: Exception = null
    while (attempts < maxAttempts) {
      attempts += 1
      try {
        val future = actor.ask(message)(timeout)
        val result = Await.result(future, timeout)
        if (result == null) {
          throw new SparkException("Actor returned null")
        }
        return result.asInstanceOf[T]
      } catch {
        case ie: InterruptedException => throw ie
        case e: Exception =>
          lastException = e
          logWarning(s"Error sending message [message = $message] in $attempts attempts", e)
      }
      if (attempts < maxAttempts) {
        Thread.sleep(retryInterval)
      }
    }

    throw new SparkException(
      s"Error sending message [message = $message]", lastException)
  }

  def protocol(actorSystem: ActorSystem): String = {
    val akkaConf = actorSystem.settings.config
    val sslProp = "akka.remote.netty.tcp.enable-ssl"
    protocol(akkaConf.hasPath(sslProp) && akkaConf.getBoolean(sslProp))
  }

  def protocol(ssl: Boolean = false): String = {
    if (ssl) {
      "akka.ssl.tcp"
    } else {
      "akka.tcp"
    }
  }

  def address(
               protocol: String,
               systemName: String,
               host: String,
               port: Any,
               actorName: String): String = {
    s"$protocol://$systemName@$host:$port/user/$actorName"
  }

}
