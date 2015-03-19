package com.advancedtelematic.feed

import java.io.File
import java.util.Date

import akka.actor.{Props, Actor}
import com.typesafe.config.ConfigFactory
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.ActorHelper
import org.apache.spark.{SparkEnv, SparkContext, SparkConf}
import org.joda.time.DateTime

class SparkReceiver(files: Array[File]) extends Actor with ActorHelper {

  val feeders = files.map { file => context.actorOf(Feeder.props( file, None , self), file.getName) }

  override def receive: Receive = {
    case e: TraceEntry =>
      store( e )
  }
}

case class TraceByCar(id: String, year: Int, month: Int, day: Int, hour: Int, date: Date, lat: BigDecimal, lng: BigDecimal, isOccupied: Boolean)

case class TraceByTime(year: Int, month: Int, day: Int, hour: Int, minute: Int, id: String, lat: BigDecimal, lng: BigDecimal, isOccupied: Boolean)

case class PickupDropoff(year: Int, date: Date, id: String, lat: BigDecimal, lng: BigDecimal, isPickup: Boolean)

object TraceByCar {
  def fromTraceEntry( te: TraceEntry ) = {
    TraceByCar(
      id = te.id,
      year = te.timestamp.year().get,
      month = te.timestamp.monthOfYear().get,
      day = te.timestamp.dayOfMonth().get,
      hour = te.timestamp.hourOfDay().get(),
      date = te.timestamp.toDate(),
      lat = te.lat,
      lng = te.lng,
      isOccupied = te.isOccupied
    )
  }
}

object TraceByTime {
  def fromTraceEntry(te : TraceEntry) = {
    TraceByTime(
      id = te.id,
      year = te.timestamp.year().get,
      month = te.timestamp.monthOfYear().get,
      day = te.timestamp.dayOfMonth().get,
      hour = te.timestamp.hourOfDay().get(),
      minute = te.timestamp.minuteOfHour().get,
      lat = te.lat,
      lng = te.lng,
      isOccupied = te.isOccupied
    )
  }
}

object CassandraFeeder extends App {

  import com.datastax.spark.connector._

  val conf = ConfigFactory.load()

  val sparkCfg = new SparkConf()
    .setMaster("local[2]").setAppName("Test")
    .set("spark.executor.memory", "4g")
    .set("spark.cores.max", "2")
    .set("spark.cassandra.connection.host", conf.getString("cassandra.host"))
    .set("spark.streaming.unpersist", "false")
  val sc = new SparkContext(sparkCfg)


  val dataFolder = new File( conf.getString("data.dir") )
  val feedFiles = if (conf.hasPath("feeds.limit")) dataFolder.listFiles().take( conf.getInt("feeds.limit") ) else dataFolder.listFiles()

  import org.apache.spark.mllib.rdd.RDDFunctions.fromRDD

  val parsedFeeds = feedFiles.map { file =>
    sc.textFile( file.getAbsolutePath )
      .map( line => TraceEntry.parse( file.getName)( line ))//.persist()
  }

  parsedFeeds.foreach( _.map(TraceByCar.fromTraceEntry).saveToCassandra("rvi_demo", "trace_by_car"))

  parsedFeeds.foreach( _.map(TraceByTime.fromTraceEntry).filter(_.minute % 5 == 0).saveToCassandra("rvi_demo", "traces_by_time") )

  parsedFeeds.foreach( rdd =>
    rdd.sliding(2)
      .filter{
        case Array(first, second) => first.isOccupied != second.isOccupied
      }.map {
        case Array(_, second) => PickupDropoff(
          year = second.timestamp.getYear,
          date = second.timestamp.toDate(),
          id = second.id,
          lat = second.lat,
          lng = second.lng,
          isPickup = second.isOccupied
        )
      }.saveToCassandra("rvi_demo", "pickups_dropoffs")
  )

}
