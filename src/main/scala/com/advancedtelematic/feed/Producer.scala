/**
* Copyright (C) 2015, Jaguar Land Rover
* This Source Code Form is subject to the terms of the Mozilla Public
* License, v. 2.0. If a copy of the MPL was not distributed with this
* file, You can obtain one at http://mozilla.org/MPL/2.0/.
*/
package com.advancedtelematic.feed

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.Props
import kafka.producer.Producer
import play.api.libs.json.Json

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
    case msg : (TraceEntry, BigDecimal) =>
      import TraceEntry.TraceAndSpeedWrites
      producer.send(new kafka.producer.KeyedMessage("gps_trace", msg._1.id, Json.stringify( Json.toJson( msg ) )))
  }

  override def postStop() {
    producer.close()
  }
}
