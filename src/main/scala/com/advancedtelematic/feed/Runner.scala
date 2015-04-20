/**
 * Copyright 2015, ATS Advanced Telematic Systems GmbH
 * All Rights Reserved
 */
package com.advancedtelematic.feed

import akka.actor._
import java.io.File

import akka.routing.{RoundRobinGroup, RoundRobinPool, SmallestMailboxPool}

import scala.collection.mutable.ArrayBuffer

class Reaper(router: ActorRef, kafkaWorkers: Seq[ActorRef], feeds: Array[ActorRef]) extends Actor with ActorLogging {

  val watched = feeds.to[ArrayBuffer]
  val watchedWorkers = kafkaWorkers.to[ArrayBuffer]

  override def preStart() = watched.foreach( context.watch )

  def watchFeeds : Receive = {
    case Terminated(ref) =>
      watched -= ref
      if (watched.isEmpty) {
        log.info( "Feeders empty." )
        context watch router
        context become watchRouter
        router ! PoisonPill
      }
  }

  def watchRouter : Receive = {
    case Terminated(`router`) =>
      log.info("All messages routed to workers")
      context become watchWorkers
      watchedWorkers.foreach(context.watch)
      watchedWorkers.foreach(_ ! PoisonPill)
  }

  def watchWorkers : Receive = {
    case Terminated(ref) =>
      watchedWorkers -= ref
      if( watchedWorkers.isEmpty ) {
        log.info( "All messages enqueued." )
        context.system.shutdown()
      }
  }

  final def receive = watchFeeds
}

object Runner extends App {

  implicit val akka = ActorSystem( "data-feeder" )
  import akka.dispatcher

  val conf = akka.settings.config

  val kafkaWorkers = (1 to 3).map( nr => akka.actorOf( KafkaProducer.props( conf.getString("kafka.broker")).withDispatcher("pinned-dispatcher"), s"kafka-producer-$nr" ) )
  val producer = akka.actorOf( RoundRobinGroup( kafkaWorkers.map(_.path.toString) ).props(), "kafka" )
//  val producer = akka.actorOf( KafkaProducer.props( conf.getString("kafka.broker")).withDispatcher("kafka-dispatcher"), "kafka" )

  val dataFolder = new File( conf.getString("data.dir") )
  akka.log.info( s"Reading data from $dataFolder" )
  val feedFiles = if (conf.hasPath("feeds.limit")) dataFolder.listFiles().take( conf.getInt("feeds.limit") ) else dataFolder.listFiles()
  val speed = conf.getInt("feeds.speed")
  val feeders = feedFiles.map { file => akka.actorOf(Feeder.props( file, if(speed == 0) None else Some(speed) , producer), file.getName) }

  val reaper = akka.actorOf( Props(classOf[Reaper], producer, kafkaWorkers, feeders), "reaper" )
}
