package com.advancedtelematic.geo

import spire.algebra.Trig
import spire.math.Fractional

case class GeoPos[T: Trig : Fractional](lat: T, lng: T)

object GeoPos {

  val R = 6371009

  def create(lat: Int, lng: Int, precision: Int): GeoPos[BigDecimal] = {
    import spire.implicits._
    import spire.math._
    val div = 10.pow(precision)
    new GeoPos(Fractional[BigDecimal].fromInt(lat) / div, Fractional[BigDecimal].fromInt(lng) / div)
  }

  import spire.implicits._
  import spire.math._

  def deltaLambda[T: Trig : Fractional]( pos1: GeoPos[T], pos2: GeoPos[T] ) = {
    val delta : T = pos2.lng - pos1.lng
    Trig[T].toRadians(delta)
  }

  def distance[T : Fractional]( from: GeoPos[T], to: GeoPos[T] )(implicit trig : Trig[T]) : T = {
    val phi1 = trig.toRadians( from.lat )
    val phi2 = trig.toRadians( to.lat )
    val deltaPhi = trig.toRadians( to.lat - from.lat )

    val a = sin( deltaPhi / 2 ).pow( 2 ) + cos( phi1 ) * cos( phi2 ) * sin( deltaLambda(from, to) / 2).pow( 2 )
    val c = 2 * atan2( sqrt(a), sqrt(Fractional[T].fromInt( 1 ) - a) )
    R * c
  }

  def bearing[T : Trig : Fractional]( from: GeoPos[T], to: GeoPos[T] ) : T = {
    val phi1 = Trig[T].toRadians( from.lat )
    val phi2 = Trig[T].toRadians( to.lat )
    val y = sin( deltaLambda( from, to ) ) * cos( phi2 )
    val x = cos( phi1 ) * sin( phi2 ) - sin( phi1 ) * cos( phi2 ) * cos( deltaLambda( from, to ) )
    atan2( y, x )
  }


  def destinationPoint[T : Trig : Fractional]( start: GeoPos[T], distance: T, brng : T ) : GeoPos[T] = {
    val phi1 = Trig[T].toRadians( start.lat )
    val lambda1 = Trig[T].toRadians( start.lng )
    val angularD = distance / R
    val phi2 = asin( sin( phi1 ) * cos( angularD ) + cos( phi1 ) * sin( angularD ) * cos( brng ) )
    val lambda2 = lambda1 + atan2( sin( brng ) * sin(angularD) * cos( phi1 ), cos( angularD ) - sin( phi1 ) * sin( phi2 ) )
    GeoPos( Trig[T].toDegrees(phi2), Trig[T].toDegrees(lambda2)  )
  }
}
