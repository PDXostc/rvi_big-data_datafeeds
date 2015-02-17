/**
 * Copyright 2015, ATS Advanced Telematic Systems GmbH
 * All Rights Reserved
 */
package com.advancedtelematic.feed

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import java.io.File
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._
import scala.util.Try

case class TraceEntry(id: String, timestamp: DateTime, payload: String)

object Feeder {

  def props(input : java.io.File, speed: Option[Int], subscriber: ActorRef) = Props( classOf[Feeder], input, speed, subscriber )

}

class Feeder(input : java.io.File, maybeSpeed: Option[Int], subscriber: ActorRef) extends Actor with akka.actor.ActorLogging {

  var src: io.Source = _
  var data : Iterator[TraceEntry] = _

  def parse(str: String) : TraceEntry = {
    val fields = str.split(" ")
    TraceEntry(
      id = input.getName(),
      timestamp = new DateTime( fields(3).toLong * 1000 ),
      payload = str
    )
  }

  override def preStart() {
    src = io.Source.fromFile(input, "utf-8")
    data = src.getLines().map( parse )
    val first = data.next()
    self ! first
  }

  def receive : Receive = {
    case entry : TraceEntry =>
      subscriber ! entry
      if( data.hasNext ) {
        val next = data.next()
        scheduleNext( entry, next )
      } else {
        log.info( "Input stream empty. Stopping." )
        context.stop( self )
      }
  }

  def scheduleThrottled(speed: Int) : (TraceEntry, TraceEntry) => Unit = (entry, next) => {
    import com.github.nscala_time.time.Imports._
    val interval = (entry.timestamp to next.timestamp).millis / speed
    context.system.scheduler.scheduleOnce(interval.millis, self, next)(context.dispatcher)
  }

  lazy val scheduleNext : (TraceEntry, TraceEntry) => Unit = maybeSpeed.map( scheduleThrottled ).getOrElse( (entry: TraceEntry, next: TraceEntry) => self ! next )


  override def postStop() {
    Try(src.close())
  }
}
