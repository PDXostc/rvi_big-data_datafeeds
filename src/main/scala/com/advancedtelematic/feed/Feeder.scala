/**
 * Copyright 2015, ATS Advanced Telematic Systems GmbH
 * All Rights Reserved
 */
package com.advancedtelematic.feed

import java.io.FileInputStream

import akka.actor.{Actor, ActorRef, Props}
import com.github.nscala_time.time.Imports._
import net.jpountz.lz4.{LZ4Factory, LZ4BlockInputStream}
import spire.algebra.Trig
import spire.math._
import scala.concurrent.duration._
import scala.util.Try


object Feeder {

  def props(input : java.io.File, speed: Option[Int], subscriber: ActorRef) = Props( classOf[Feeder], input, speed, subscriber )

  case class GpsPos( lat: BigDecimal, lng: BigDecimal )

  import spire.implicits._

  def deltaLambda( pos1: GpsPos, pos2: GpsPos ) = {
    val delta = pos2.lng - pos1.lng
    Trig[BigDecimal].toRadians(delta)
  }

  val R = BigDecimal( 6371009 )

  def distance( from: GpsPos, to: GpsPos )(implicit trig : Trig[BigDecimal]) : BigDecimal = {
    val phi1 = trig.toRadians( from.lat )
    val phi2 = trig.toRadians( to.lat )
    val deltaPhi = trig.toRadians( to.lat - from.lat )

    val a = sin( deltaPhi / 2 ).pow( 2 ) + cos( phi1 ) * cos( phi2 ) * sin( deltaLambda(from, to) / 2).pow( 2 )
    val c = 2 * atan2( sqrt(a), sqrt(BigDecimal( 1 ) - a) )
    R * c
  }

  implicit def entryPos( entry : TraceEntry ) : GpsPos = GpsPos( entry.lat, entry.lng )

  def speed( from : TraceEntry, to: TraceEntry ) : BigDecimal = {
    import com.github.nscala_time.time.Imports._
    val l = distance( from, to )
    val time = (from.timestamp to to.timestamp).toDuration.getStandardSeconds
    (l / time) * 1000 / 360
  }

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

  val Zero = BigDecimal( 0 )

  def running( prev : Option[TraceEntry] ) : Receive = {
    case entry : TraceEntry =>
      subscriber ! (entry.copy( timestamp = entry.timestamp + timeDelta), prev.map( Feeder.speed(_, entry) ).getOrElse( Zero ))
      if( data.hasNext ) {
        context become running( Some(entry) )
        val next = data.next()
        scheduleNext( entry, next )
      } else {
        log.info( "Input stream empty. Stopping." )
        context.stop( self )
      }
  }

  def receive : Receive = running( None )

  def scheduleThrottled(speed: Int) : (TraceEntry, TraceEntry) => Unit = (entry, next) => {
    val interval = (entry.timestamp to next.timestamp).millis / speed
    context.system.scheduler.scheduleOnce(interval.millis, self, next)(context.dispatcher)
  }

  lazy val scheduleNext : (TraceEntry, TraceEntry) => Unit = maybeSpeed.map( scheduleThrottled ).getOrElse( (entry: TraceEntry, next: TraceEntry) => self ! next )


  override def postStop() {
    Try(src.close())
  }
}
