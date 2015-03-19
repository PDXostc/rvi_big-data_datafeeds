/**
 * Copyright 2015, ATS Advanced Telematic Systems GmbH
 * All Rights Reserved
 */
package com.advancedtelematic.feed

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.Props
import kafka.producer.Producer

object KafkaProducer {
  def props( broker : String ) = Props( classOf[KafkaProducer], broker)
}

class KafkaProducer(broker : String) extends Actor with ActorLogging {

  val producer = {
    val props = new java.util.Properties()
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("metadata.broker.list", broker)
    new Producer[String, String]( new kafka.producer.ProducerConfig(props) )
  }

  def receive = {
    case event : TraceEntry =>
      producer.send(new kafka.producer.KeyedMessage("gps_trace", event.id, event.toCsv()))
  }

  override def postStop() {
    producer.close()
  }
}
