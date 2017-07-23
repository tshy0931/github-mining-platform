package com.gitmining.platform.data.ingest

import java.util.Properties
import java.util.concurrent.TimeUnit

import akka.actor.Actor
import com.gitmining.platform.data.ingest.DataConsumer.{PollTimeout, Stop, Subscribe}

import scala.collection.JavaConverters._
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}

import scala.annotation.tailrec

/**
  * Created by weiwang on 23/07/17.
  */
object DataConsumer {

  final case object Stop
  final case class Subscribe[K,V](f:ConsumerRecord[K,V]=>Any, topics: String*)
  final case class Poll[K,V](f:ConsumerRecord[K,V] => Any)
  final case class PollTimeout(timeoutMillis:Long)

  implicit val pollTimeout = PollTimeout(10000)

  def of[K,V](props:Properties) = {
    new KafkaConsumer[K, V](props)
  }
}

class DataConsumer[K,V] extends Actor {

  import DataConsumer._

  val conf = ConfigFactory.load()
  val props = new Properties()

  props.put("bootstrap.servers", conf.getString("gitmining-crawl-akka.kafka.server"))
  props.put("group.id", conf.getString("gitmining-crawl-akka.kafka.group-id"))
  props.put("key.deserializer", conf.getString("gitmining-crawl-akka.kafka.key-deserializer"))
  props.put("value.deserializer", conf.getString("gitmining-crawl-akka.kafka.value-deserializer"))

  lazy val consumer = DataConsumer.of[K,V](props)

  def close = consumer.close(30, TimeUnit.SECONDS)

  override def receive: Receive = {
    case Subscribe(f, topics) =>
      subscribe(topics)(f)
    case x:Poll[K,V] =>
      val records = consumer.poll(pollTimeout.timeoutMillis)
      records.asScala foreach { record =>
        x.f(record)
      }
    case Stop =>
      consumer.close(30, TimeUnit.SECONDS)
  }

  private def subscribe(topics: String*)(f:ConsumerRecord[K,V] => Any) = {
    consumer.subscribe(List(topics: _*).asJava)

  }

  /**
    * Recursively poll consumer to process messages from Kafka.
    * @param hasNext determine whether call next poll recursively. If false will stop polling.
    * @param timeToWait time to wait before calling next poll recursively.
    * @param f function to apply on polled ConsumerRecord[K,V]
    * @param timeout The time, in milliseconds, spent waiting in poll if data is not available in the buffer.
    *                   If 0, returns immediately with any records that are available currently in the buffer,
    *                   else returns empty. Must not be negative.
    */
  @tailrec
  private def poll(hasNext:Boolean, timeToWait:ConsumerRecord[K,V] => Long)
                  (f:ConsumerRecord[K,V] => Any)
                  (implicit timeout:PollTimeout):Unit = hasNext match {
    case true =>
      val records = consumer.poll(timeout.timeoutMillis)
      records.asScala foreach { record =>
        f(record)
      }
      poll(hasNext, timeToWait)(f)
    case false => Unit

  }

}
