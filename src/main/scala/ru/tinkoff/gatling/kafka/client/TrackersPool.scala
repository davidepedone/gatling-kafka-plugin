package ru.tinkoff.gatling.kafka.client

import akka.actor.{ActorSystem, CoordinatedShutdown}
import io.gatling.commons.util.Clock
import io.gatling.core.stats.StatsEngine
import io.gatling.core.util.NameGen
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.serialization.Serdes._
import ru.tinkoff.gatling.kafka.KafkaLogging
import ru.tinkoff.gatling.kafka.client.KafkaMessageTrackerActor.MessageConsumed
import ru.tinkoff.gatling.kafka.protocol.KafkaProtocol.KafkaMatcher
import ru.tinkoff.gatling.kafka.request.KafkaProtocolMessage
import ru.tinkoff.gatling.kafka.streaming.{CustomProcessor, KafkaStreamStateListener}

import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters._

class TrackersPool(
    streamsSettings: Map[String, AnyRef],
    system: ActorSystem,
    statsEngine: StatsEngine,
    clock: Clock,
) extends KafkaLogging with NameGen {

  private val trackers = new ConcurrentHashMap[String, KafkaMessageTracker]
  private val props    = new java.util.Properties()
  props.putAll(streamsSettings.asJava)

  def getTracker(topic: String): KafkaMessageTracker = trackers.get(topic)
  def tracker(
      inputTopic: String,
      outputTopic: String,
      messageMatcher: KafkaMatcher,
      responseTransformer: Option[KafkaProtocolMessage => KafkaProtocolMessage],
      streamStateListener: KafkaStreamStateListener,
  ): KafkaMessageTracker =
    trackers.computeIfAbsent(
      outputTopic,
      _ => {
        val actor =
          system.actorOf(KafkaMessageTrackerActor.props(statsEngine, clock), genName("kafkaTrackerActor"))

        val builder = new StreamsBuilder
        logger.info(s"Building KafkaStream on topic $outputTopic")

        builder
          .stream[Array[Byte], Array[Byte]](outputTopic)
          .process(
            CustomProcessor
              .supplier[Array[Byte], Array[Byte]](inputTopic, outputTopic, actor, messageMatcher, responseTransformer),
          )
        val streams = new KafkaStreams(builder.build(), props)

        streams.setStateListener(streamStateListener)
        streams.cleanUp()
        streams.start()

        CoordinatedShutdown(system).addJvmShutdownHook(streams.close())

        new KafkaMessageTracker(actor)
      },
    )
}
