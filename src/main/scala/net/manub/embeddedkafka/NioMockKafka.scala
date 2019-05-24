package net.manub.embeddedkafka

import net.manub.embeddedkafka.ops.{ConsumerOps, ProducerOps}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{Deserializer, Serializer, StringSerializer}

import scala.concurrent.duration._

trait NioMockKafka extends ConsumerOps[EmbeddedKafkaConfig] with ProducerOps[EmbeddedKafkaConfig] {
  override private[embeddedkafka] def baseConsumerConfig(implicit config: EmbeddedKafkaConfig): Map[String, Object] = ???

  override private[embeddedkafka] def baseProducerConfig(implicit config: EmbeddedKafkaConfig): Map[String, Object] = ???

  override def consumeNumberKeyedMessagesFromTopics[K, V](
    topics: Set[String],
    number: Int,
    autoCommit: Boolean = false,
    timeout: Duration = 5.seconds,
    resetTimeoutOnEachMessage: Boolean = true
  )(
    implicit config: EmbeddedKafkaConfig,
    keyDeserializer: Deserializer[K],
    valueDeserializer: Deserializer[V]
  ): Map[String, List[(K, V)]] = {
    get(topics, number, keyDeserializer, valueDeserializer)
  }

  override private[embeddedkafka] def defaultProducerConf(implicit config: EmbeddedKafkaConfig): Map[String, Object] = ???

  override def kafkaProducer[K, T](topic: String, key: K, message: T)
    (implicit config: EmbeddedKafkaConfig, keySerializer: Serializer[K], serializer: Serializer[T]): KafkaProducer[K, T] = ???

  override def publishToKafka[T](topic: String, message: T)
    (implicit config: EmbeddedKafkaConfig, serializer: Serializer[T]): Unit =
    put(new StringSerializer(), serializer, new ProducerRecord[String, T](topic, message))

  override def publishToKafka[T](producerRecord: ProducerRecord[String, T])
    (implicit config: EmbeddedKafkaConfig, serializer: Serializer[T]): Unit =
    put(new StringSerializer(), serializer, producerRecord)

  override def publishToKafka[K, T](topic: String, key: K, message: T)
    (implicit config: EmbeddedKafkaConfig, keySerializer: Serializer[K], serializer: Serializer[T]): Unit =
    put(keySerializer, serializer, new ProducerRecord[K, T](topic, key, message))

  override def publishToKafka[K, T](topic: String, messages: Seq[(K, T)])
    (implicit config: EmbeddedKafkaConfig, keySerializer: Serializer[K], serializer: Serializer[T]): Unit =
    messages.foreach { case (k, v) => publishToKafka(topic, k, v) }

  def put[K, T](kSer: Serializer[K], mSer: Serializer[T], record: ProducerRecord[K, T]): Unit
  def get[K, T](topics: Set[String], num: Int, keyDeserializer: Deserializer[K], valueDeserializer: Deserializer[T]): Map[String, List[(K, T)]]
}
