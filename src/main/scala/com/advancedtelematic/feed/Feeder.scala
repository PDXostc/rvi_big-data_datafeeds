/**
 * Copyright 2015, ATS Advanced Telematic Systems GmbH
 * All Rights Reserved
 */
package com.advancedtelematic.feed

import java.io.FileInputStream

import akka.actor.{Actor, ActorRef, Props}
import com.github.nscala_time.time.Imports._
import net.jpountz.lz4.{LZ4Factory, LZ4BlockInputStream}
import scala.concurrent.duration._
import scala.util.Try


object Feeder {

  def props(input : java.io.File, speed: Option[Int], subscriber: ActorRef) = Props( classOf[Feeder], input, speed, subscriber )

}

class Feeder(input : java.io.File, maybeSpeed: Option[Int], subscriber: ActorRef) extends Actor with akka.actor.ActorLogging {
  import org.joda.time.{Duration => JodaDuration}

  var src: scala.io.Source = _
  var data : Iterator[TraceEntry] = _
  var timeDelta: JodaDuration = _

  override def preStart() {
    src = scala.io.Source.fromInputStream( new LZ4BlockInputStream( new FileInputStream(input) ) , "utf-8")
    data = src.getLines().map( TraceEntry.parse(input.getName()) )
    val first = data.next()
    timeDelta = (first.timestamp to DateTime.now).toDuration
    self ! first
  }

  def receive : Receive = {
    case entry : TraceEntry =>
      subscriber ! entry.copy( timestamp = entry.timestamp + timeDelta)
      if( data.hasNext ) {
        val next = data.next()
        scheduleNext( entry, next )
      } else {
        log.info( "Input stream empty. Stopping." )
        context.stop( self )
      }
  }

  def scheduleThrottled(speed: Int) : (TraceEntry, TraceEntry) => Unit = (entry, next) => {
    val interval = (entry.timestamp to next.timestamp).millis / speed
    context.system.scheduler.scheduleOnce(interval.millis, self, next)(context.dispatcher)
  }

  lazy val scheduleNext : (TraceEntry, TraceEntry) => Unit = maybeSpeed.map( scheduleThrottled ).getOrElse( (entry: TraceEntry, next: TraceEntry) => self ! next )


  override def postStop() {
    Try(src.close())
  }
}
