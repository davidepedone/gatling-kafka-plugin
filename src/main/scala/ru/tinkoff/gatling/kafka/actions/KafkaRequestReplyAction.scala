package ru.tinkoff.gatling.kafka.actions

import io.gatling.commons.stats.KO
import io.gatling.commons.util.Clock
import io.gatling.commons.validation._
import io.gatling.core.Predef.pause
import io.gatling.core.action.{Action, RequestAction}
import io.gatling.core.controller.throttle.Throttler
import io.gatling.core.session.el._
import io.gatling.core.session.{Expression, Session}
import io.gatling.core.stats.StatsEngine
import io.gatling.core.util.NameGen
import org.apache.kafka.common.serialization.Serializer
import ru.tinkoff.gatling.kafka.KafkaLogging
import ru.tinkoff.gatling.kafka.protocol.KafkaComponents
import ru.tinkoff.gatling.kafka.request.KafkaProtocolMessage
import ru.tinkoff.gatling.kafka.request.builder.KafkaRequestReplyAttributes
import ru.tinkoff.gatling.kafka.streaming.KafkaStreamStateListener

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.reflect.{ClassTag, classTag}

class KafkaRequestReplyAction[K: ClassTag, V: ClassTag](
    components: KafkaComponents,
    attributes: KafkaRequestReplyAttributes[K, V],
    val statsEngine: StatsEngine,
    val clock: Clock,
    val next: Action,
    throttler: Option[Throttler],
) extends RequestAction with KafkaLogging with NameGen {
  override def requestName: Expression[String] = attributes.requestName

  override def sendRequest(session: Session): Validation[Unit] = {
    for {
      rn  <- requestName(session)
      msg <- resolveToProtocolMessage(session)
    } yield throttler
      .fold(publishAndLogMessage(rn, msg, session))(
        _.throttle(session.scenario, () => publishAndLogMessage(rn, msg, session)),
      )

  }

  private def serializeKey(
      serializer: Serializer[K],
      keyE: Expression[K],
      topicE: Expression[String],
  ): Expression[Array[Byte]] = s =>
    // need for work gatling Expression Language
    if (classTag[K].runtimeClass.getCanonicalName == "java.lang.String")
      for {
        topic <- topicE(s)
        key   <- keyE.asInstanceOf[Expression[String]](s).flatMap(_.el[String].apply(s))
      } yield serializer.asInstanceOf[Serializer[String]].serialize(topic, key)
    else
      for {
        topic <- topicE(s)
        key   <- keyE(s)
      } yield serializer.serialize(topic, key)

  private val kafkaStreamStateListener = new KafkaStreamStateListener

  private def optToVal[T](ovt: Option[Validation[T]]): Validation[Option[T]] =
    ovt.fold(Option.empty[T].success)(_.map(Option[T]))

  private def resolveToProtocolMessage: Expression[KafkaProtocolMessage] = s =>
    // need for work gatling Expression Language
    if (classTag[V].runtimeClass.getCanonicalName == "java.lang.String")
      for {
        key         <- serializeKey(attributes.keySerializer, attributes.key, attributes.inputTopic)(s)
        inputTopic  <- attributes.inputTopic(s)
        outputTopic <- attributes.outputTopic(s)
        value       <- attributes.value
                         .asInstanceOf[Expression[String]](s)
                         .flatMap(_.el[String].apply(s))
                         .map(v => attributes.valueSerializer.asInstanceOf[Serializer[String]].serialize(inputTopic, v))
        headers     <- optToVal(attributes.headers.map(_(s)))
      } yield KafkaProtocolMessage(key, value, inputTopic, outputTopic, headers)
    else
      for {
        key         <- serializeKey(attributes.keySerializer, attributes.key, attributes.inputTopic)(s)
        inputTopic  <- attributes.inputTopic(s)
        outputTopic <- attributes.outputTopic(s)
        value       <- attributes.value(s).map(v => attributes.valueSerializer.serialize(inputTopic, v))
        headers     <- optToVal(attributes.headers.map(_(s)))
      } yield KafkaProtocolMessage(key, value, inputTopic, outputTopic, headers)

  private def publishAndLogMessage(requestNameString: String, msg: KafkaProtocolMessage, session: Session): Unit = {
    components.trackersPool.tracker(
      msg.inputTopic, msg.outputTopic, components.kafkaProtocol.messageMatcher, None, kafkaStreamStateListener
    )

    do {
      logger.trace(s"Waiting for stream to be ready, current status: ${kafkaStreamStateListener.getStatus}")
      pause(Duration(300, TimeUnit.SECONDS))
    } while (!kafkaStreamStateListener.isRunning)

    val now = clock.nowMillis
    components.sender.send(msg)(
      rm => {
        if (logger.underlying.isDebugEnabled) {
          logMessage(s"Record sent user=${session.userId} key=${new String(msg.key)} topic=${rm.topic()}", msg)
        }
        msg.timestamp = rm.timestamp()
        val id = components.kafkaProtocol.messageMatcher.requestMatch(msg)
        components.trackersPool
          .getTracker(msg.outputTopic)
          .track(
            id,
            rm.timestamp(),
            components.kafkaProtocol.timeout.toMillis,
            attributes.checks,
            session,
            next,
            requestNameString,
          )
      },
      e => {
        logger.error(e.getMessage, e)
        statsEngine.logResponse(
          session.scenario,
          session.groups,
          requestNameString,
          now,
          clock.nowMillis,
          KO,
          Some("500"),
          Some(e.getMessage),
        )
      },
    )
  }

  override def name: String = genName("kafkaRequestReply")
}
