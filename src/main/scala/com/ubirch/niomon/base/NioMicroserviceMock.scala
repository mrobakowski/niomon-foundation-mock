package com.ubirch.niomon.base

import java.util.UUID
import java.util.concurrent.TimeoutException

import akka.Done
import akka.kafka.scaladsl.Consumer.{DrainingControl, NoopControl}
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.Logger
import com.ubirch.niomon.util.{KafkaPayload, KafkaPayloadFactory}
import com.ubirch.kafka._
import com.ubirch.niomon.cache.RedisCache
import net.manub.embeddedkafka.NioMockKafka
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{Deserializer, Serializer, StringDeserializer, StringSerializer}
import org.mockito.MockitoSugar
import org.mockito.stubbing.ReturnsDeepStubs
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.util.Try

/**
 * Implementation of NioMicroservice for use in tests of different NioMicroserviceLogics. Redis and Kafka are mocked,
 * which makes the tests fairly fast.
 */
class NioMicroserviceMock[I, O](logicFactory: NioMicroservice[I, O] => NioMicroserviceLogic[I, O])(implicit
  inputPayloadFactory: KafkaPayloadFactory[I],
  outputPayloadFactory: KafkaPayloadFactory[O]
) extends NioMicroservice[I, O] {
  var name: String = s"nio-microservice-mock-${UUID.randomUUID()}"
  protected val logger: Logger = Logger(LoggerFactory.getLogger(getClass.getName + s"($name)"))
  var errorTopic: Option[String] = None
  var outputTopics: Map[String, String] = Map()
  var config: Config = ConfigFactory.empty()
  val redisCache: RedisCache = {
    val mock = MockitoSugar.mock[RedisCache](ReturnsDeepStubs)
    mock
  }

  override def context: NioMicroservice.Context = new NioMicroservice.Context(redisCache, config)

  lazy val inputPayload: KafkaPayload[I] = inputPayloadFactory(context)
  lazy val outputPayload: KafkaPayload[O] = outputPayloadFactory(context)
  lazy val logic: NioMicroserviceLogic[I, O] = logicFactory(this)

  def run: DrainingControl[Done] = DrainingControl((NoopControl, Future.successful(Done)))

  var errors: Vector[ProducerRecord[String, String]] = Vector()
  var results: Vector[ProducerRecord[String, O]] = Vector()

  // This is where most of the magic happens. The `put` method basically kicks off the logic processing and then puts
  // the results into [[results]] vector. The `get` method gets the matching messages out the [[results]] vector
  // and returns them
  val kafkaMocks: NioMockKafka = new NioMockKafka {
    override def put[K, T](kSer: Serializer[K], vSer: Serializer[T], record: ProducerRecord[K, T]): Unit = {
      val serializedKey = kSer.serialize(record.topic(), record.key())
      val serializedValue = vSer.serialize(record.topic(), record.value())

      val key = new StringDeserializer().deserialize(record.topic(), serializedKey)
      val value = Try(inputPayload.deserializer.deserialize(record.topic(), record.headers(), serializedValue))

      val msg = new ConsumerRecord[String, Try[I]](record.topic(), record.partition(), 0, 0,
        null, 0L, 0, 0, key, value, record.headers())
      val processed = Try(logic.processRecord(msg.copy(value = msg.value().get)))

      processed.fold({ e =>
        errors :+= producerErrorRecordToStringRecord(
          wrapThrowableInKafkaRecord(msg, e),
          errorTopic.getOrElse("unused-topic")
        )
      }, {
        results :+= _
      })
    }

    override def get[K, T](
      topics: Set[String],
      num: Int,
      keyDeserializer: Deserializer[K],
      valueDeserializer: Deserializer[T]
    ): Map[String, List[(K, T)]] = {
      var i = 0

      def splitMatching[X](x: Vector[ProducerRecord[String, X]]): (Vector[ProducerRecord[String, X]], Vector[ProducerRecord[String, X]]) = {
        var matched = Vector[ProducerRecord[String, X]]()
        var nonMatched = Vector[ProducerRecord[String, X]]()
        x.foreach { r =>
          if (i < num && topics.contains(r.topic())) {
            matched :+= r
            i += 1
          } else {
            nonMatched :+= r
          }
        }
        (matched, nonMatched)
      }

      val (matched, nonMatched) = splitMatching(results)
      results = nonMatched

      val (matchedErrors, nonMatchedErrors) = splitMatching(errors)
      errors = nonMatchedErrors

      def processMatches[X](matched: Vector[ProducerRecord[String, X]], serializer: Serializer[X]): Map[String, List[(K, T)]] = {
        matched
          .map { pr =>
            val serializedValue = serializer.serialize(pr.topic(), pr.headers(), pr.value())
            val deserializedValue = valueDeserializer.deserialize(pr.topic(), pr.headers(), serializedValue)

            val serializedKey = new StringSerializer().serialize(pr.topic(), pr.headers(), pr.key())
            val deserializedKey = keyDeserializer.deserialize(pr.topic(), pr.headers(), serializedKey)
            (pr.topic(), deserializedKey, deserializedValue)
          }
          .groupBy(_._1)
          .map { case (key, v) => key -> v.map { x => x._2 -> x._3 }.toList }
      }

      val res = processMatches(matched, outputPayload.serializer) ++ processMatches(matchedErrors, new StringSerializer())

      // this is a timeout, because this is a mock and I'm trying to mirror the original behavior
      if (res.values.flatten.size < num) throw new TimeoutException()

      res
    }
  }
}

object NioMicroserviceMock {
  def apply[I, O](logicFactory: NioMicroservice[I, O] => NioMicroserviceLogic[I, O])(implicit
    inputPayloadFactory: KafkaPayloadFactory[I],
    outputPayloadFactory: KafkaPayloadFactory[O]
  ): NioMicroserviceMock[I, O] = new NioMicroserviceMock(logicFactory)
}