/**
 * Copyright 2015, ATS Advanced Telematic Systems GmbH
 * All Rights Reserved
 */
package com.advancedtelematic.feed

import akka.actor.ActorSystem
import akka.actor.Props
import java.io.File

object Runner extends App {

  implicit val akka = ActorSystem( "data-feeder" )
  import akka.dispatcher

  val conf = akka.settings.config
  val producer = akka.actorOf(KafkaProducer.props( conf.getString("kafka.broker")), "kafka")

  val dataFolder = new File( conf.getString("data.dir") )
  val feedFiles = if (conf.hasPath("feeds.limit")) dataFolder.listFiles().take( conf.getInt("feeds.limit") ) else dataFolder.listFiles() 
  val feeders = feedFiles.map { file => akka.actorOf(Props( classOf[Feeder], file, conf.getInt("feeds.speed"), producer)) }

}
