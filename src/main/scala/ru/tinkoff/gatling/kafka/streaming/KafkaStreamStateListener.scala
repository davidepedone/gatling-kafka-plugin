package ru.tinkoff.gatling.kafka.streaming

import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KafkaStreams.State
import ru.tinkoff.gatling.kafka.KafkaLogging

class KafkaStreamStateListener extends KafkaStreams.StateListener with KafkaLogging {

  private var currentStatus: KafkaStreams.State = State.NOT_RUNNING

  override def onChange(newStatus: State, oldStatus: State): Unit = {
    if (newStatus.equals(State.RUNNING)) {
      logger.debug("Stream is ready to consume")
    }
    currentStatus = newStatus
  }

  def isRunning: Boolean = {
    currentStatus.equals(State.RUNNING)
  }

  def getStatus: String = {
    currentStatus.name()
  }
}
