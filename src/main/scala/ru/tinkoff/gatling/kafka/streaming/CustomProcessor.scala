package ru.tinkoff.gatling.kafka.streaming

import akka.actor.ActorRef
import org.apache.kafka.streams.processor.api
import org.apache.kafka.streams.processor.api.{Processor, ProcessorSupplier}
import ru.tinkoff.gatling.kafka.KafkaLogging
import ru.tinkoff.gatling.kafka.client.KafkaMessageTrackerActor.MessageConsumed
import ru.tinkoff.gatling.kafka.protocol.KafkaProtocol.KafkaMatcher
import ru.tinkoff.gatling.kafka.request.KafkaProtocolMessage

class CustomProcessor[K, V](
    inputTopic: String,
    outputTopic: String,
    actor: ActorRef,
    messageMatcher: KafkaMatcher,
    responseTransformer: Option[KafkaProtocolMessage => KafkaProtocolMessage],
) extends Processor[K, V, K, V] with KafkaLogging {

  override def process(record: api.Record[K, V]): Unit = {
    val key     = record.key().asInstanceOf[Array[Byte]]
    val value   = record.value().asInstanceOf[Array[Byte]]
    val message = KafkaProtocolMessage(
      key,
      value,
      inputTopic,
      outputTopic,
      headers = Option.apply(record.headers()),
      timestamp = record.timestamp(),
    )

    val replyId = messageMatcher.responseMatch(message)

    if (replyId == null) {
      logger.error(s"no messageMatcher key for read message")
    } else {
      if (key == null || value == null) {
        logger.warn(s" --- received message with null key or value")
      } else {
        logger.trace(s" --- received ${new String(key)} ${new String(value)}")
      }
      val receivedTimestamp = record.timestamp()
      if (key != null) {
        logMessage(
          s"Record received key=${new String(key)}",
          message,
        )
      } else {
        logMessage(
          s"Record received key=null",
          message,
        )
      }
      actor ! MessageConsumed(
        replyId,
        receivedTimestamp,
        responseTransformer.map(_(message)).getOrElse(message),
      )
    }

  }
}

object CustomProcessor {
  def supplier[K, V](
      inputTopic: String,
      outputTopic: String,
      actor: ActorRef,
      messageMatcher: KafkaMatcher,
      responseTransformer: Option[KafkaProtocolMessage => KafkaProtocolMessage],
  ): ProcessorSupplier[K, V, K, V] =
    () => new CustomProcessor[K, V](inputTopic, outputTopic, actor, messageMatcher, responseTransformer)
}
