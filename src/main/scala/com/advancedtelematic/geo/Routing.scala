package com.advancedtelematic.geo

import java.io.File
import java.nio.file.Files

import akka.actor._
import akka.io.IO
import akka.util.Timeout
import com.advancedtelematic.feed.CassandraFeeder
import com.advancedtelematic.feed.TraceEntry
import spray.httpx.PlayJsonSupport
import scala.annotation.tailrec
import scala.collection.mutable.Buffer
import spray.can.Http
import spray.http.HttpRequest
import spray.http.HttpResponse
import spray.http.StatusCodes
import spray.http.Uri

import scala.util.Try

class PointOnStreetActor(connection: ActorRef, entry: TraceEntry) extends Actor with akka.actor.ActorLogging {

  import spray.http.HttpMethods.GET
  import spray.http.StatusCodes._
  import context._
  import spire.implicits._

  connection ! HttpRequest( GET, Uri( "/nearest" ).withQuery("loc" -> s"${entry.lat},${entry.lng}")  )

  import play.api.libs.json._
  import play.api.libs.json.Reads._
  type OsrmStatus
  implicit val StatusReads : Reads[OsrmStatus] = (__ \ "status").read[Int].map[OsrmStatus]( _.asInstanceOf[OsrmStatus] )
  implicit val PosReads : Reads[GeoPos[BigDecimal]] = (__ \ "mapped_coordinate").read[Seq[BigDecimal]].map { xs => GeoPos(xs.head, xs.tail.head) }

  override def receive = {
    case response @ HttpResponse(OK, body, _, _ ) =>
      import spray.httpx.PlayJsonSupport._
      import spray.client.pipelining._
      val status : OsrmStatus = unmarshal[OsrmStatus].apply( response )

      if( status == 0 ) {
        val pos = unmarshal[GeoPos[BigDecimal]].apply(response)
        parent ! (entry, entry.copy( lat = pos.lat, lng = pos.lng))
      } else {
//        parent ! entry
        log.error( s"Unable to find nearest point on the street for ${entry}. ${body}" )
      }
      context stop self
    case HttpResponse( status, _, _, _) =>
      log.error( s"Unexpected response from the service with the status $status" )
      context stop self
  }

}

class FixCoordinatesActor(connection: ActorRef, from: TraceEntry, to: TraceEntry) extends Actor with ActorLogging {

  val fromActor = context.actorOf(Props( classOf[PointOnStreetActor], connection, from), "from")
  val toActor = context.actorOf(Props( classOf[PointOnStreetActor], connection, to), "to")

  var fixedFrom : Option[TraceEntry] = None
  var fixedTo : Option[TraceEntry] = None

  def handleFix : Receive = {
    case (`from`, fix : TraceEntry) =>
      fixedFrom = Some(fix)
    case (`to`, fix : TraceEntry) =>
      fixedTo = Some(fix)
  }

  override def receive = handleFix.andThen { _ =>
    if ( fixedFrom.isDefined && fixedTo.isDefined ) {
      context.parent ! (fixedFrom.get, fixedTo.get)
      context stop self
    }
  }

}

class Worker(connection: ActorRef, input : java.io.File, output : java.io.File) extends Actor with akka.actor.ActorLogging {

  val source = scala.io.Source.fromFile( input, "utf-8")
  val dest = new java.io.PrintWriter(new java.io.BufferedWriter(  new java.io.FileWriter( output ) ))
  val data : Iterator[(TraceEntry, TraceEntry)] =
    source.getLines().map{line =>
      log.debug( s" $input: " + line)
      TraceEntry.parse(input.getName())(line)
    }.sliding(2, 1).map( s => s.head -> s.tail.head )

  override def preStart() = {
    log.info( s"Processing $input, writing into $output" )
    self ! data.next()
  }

  import context._
  import spray.http.HttpMethods.GET

  case class RouteSegment( start : GeoPos[BigDecimal], end : GeoPos[BigDecimal], length : BigDecimal)

  import play.api.libs.json._
  import play.api.libs.json.Writes
  val MultipointWrites : Writes[List[TraceEntry]] = Writes { xs =>
    Json.obj(
      "type" -> "MultiPoint",
      "coordinates" ->
        xs.map( x => Json.arr( x.lng, x.lat ) )

    )
  }

  def writeRoute( from : TraceEntry, to : TraceEntry, route: Seq[GeoPos[BigDecimal]]) : Unit = {
    import com.github.nscala_time.time.Imports._
    import spire.implicits._

    def cutFrom( segment : RouteSegment, distance : BigDecimal ) : Option[RouteSegment] = {
      if( segment.length == distance ) None
      else {
        val brng = GeoPos.bearing( segment.start, segment.end)
        val splitPos = GeoPos.destinationPoint(segment.start, distance, brng)
        Some( RouteSegment( splitPos, segment.end, segment.length - distance ) )
      }
    }

    def entryFrom( pos: GeoPos[BigDecimal], timeStamp: DateTime ) =
      TraceEntry( from.id, timeStamp, pos.lat, pos.lng, from.isOccupied  )

    def skip(length : BigDecimal, segments: List[RouteSegment]) : (BigDecimal, List[RouteSegment]) = segments match {
      case first :: rest =>
        if( first.length >= length) (length, segments)
        else skip( length - first.length, rest  )
      case Nil => (length, Nil)
    }

    def splitRoute( segments : List[RouteSegment], length : BigDecimal ) : List[GeoPos[BigDecimal]] = {
      @tailrec def run( segms : List[RouteSegment], acc: Buffer[GeoPos[BigDecimal]] ) : Buffer[GeoPos[BigDecimal]] = segms match {
        case Nil => acc
        case segment :: rest =>
          val updatedAcc = acc :+ segment.start
          if( segment.length >= length ) {
            run( cutFrom(segment, length).fold( rest )( _ :: rest), updatedAcc)
          } else {
            val (ln, seg) = skip( length - segment.length, rest )
            if( seg.isEmpty ) acc :+ segment.end
            else run( cutFrom( seg.head, ln ).fold( seg.tail )( _ :: seg.tail ), updatedAcc)
          }
      }
      run(segments, Buffer.empty[GeoPos[BigDecimal]]).toList
    }

    val duration = (from.timestamp to to.timestamp).toDuration.getStandardSeconds
    val completeRoute : Seq[GeoPos[BigDecimal]] = GeoPos( from.lat, from.lng ) +: route :+ GeoPos( to.lat, to.lng )

    log.debug( s"Routing from $from to $to" )
    val entries = if( duration > 5 ) {
      val segments = completeRoute.sliding(2, 1).map { seq =>
        val start = seq.head
        val dest = seq.tail.head
        val length = GeoPos.distance(start, dest)
        RouteSegment(start, dest, length)
      }.toList
      val distance = segments.map( _.length ).sum
      val speed = distance / duration // meters /sec
      val parts : Long = duration / 5
      val partLength = distance / parts

      if( distance < 10 || duration > 600) {
        log.debug( s"Not moving? Distance: $distance, duration: $duration, speed: $speed")
        List(from, to)
      } else {
        splitRoute(segments.toList, partLength).zipWithIndex.map {
          case (pos, index) =>
            TraceEntry( from.id, from.timestamp + (5 * index).seconds, pos.lat, pos.lng, from.isOccupied )
        }
      }
    } else List( from, to )
//    log.info( s"Route from $from to $to")
//    log.info( Json.prettyPrint( Json.toJson(entries)(MultipointWrites) ) )
    entries.tail.foreach( x =>
      dest.println( s"${x.lat} ${x.lng} ${x.isOccupied} ${x.timestamp.getMillis / 1000}" )
    )
  }

  def fixCoordinates( from: TraceEntry, to : TraceEntry ) = {
    context.actorOf(Props( classOf[FixCoordinatesActor], connection, from, to), "fix")
    context become receive
  }

  def waitForRoute(from: TraceEntry, to : TraceEntry) : Receive = {
    case response @ HttpResponse( StatusCodes.OK, body, _, _ ) =>
      import RoutingService._
      import spray.client.pipelining._
      import spray.httpx.PlayJsonSupport._
      val status = unmarshal[Status].apply(response)
      if( status.code != 0 ) {
        log.debug( s"Error finding route from $from to $to, message: ${status.message}" )
        dest.println( s"${to.lat} ${to.lng} ${to.isOccupied} ${to.timestamp.getMillis / 1000}" )
//        fixCoordinates(from, to)
      } else {
        val geometry = unmarshal[Route].apply(response).routeGeometry
        writeRoute( from, to, geometry)
      }
      if( data.hasNext ) {
        val next = data.next()
        context become receive
        self ! next
      } else {
        log.info( "Input stream empty. Stopping." )
        context.stop( self )
      }
    case HttpResponse( status, _, _, _) =>
      log.error(s"Unexpected response: $status")
      context stop self
  }

  override def postStop(): Unit = {
    source.close()
    dest.flush()
    dest.close()
    Files.delete( input.toPath )
  }

  override def receive : Receive = {
    case (from: TraceEntry, to : TraceEntry) =>
      connection ! HttpRequest( GET, Uri("/viaroute").withQuery("loc" -> s"${from.lat},${from.lng}", "loc" -> s"${to.lat},${to.lng}") )
      context.become( waitForRoute(from, to) )
  }
}

class JobScheduler(connector : ActorRef, files: Array[File], outputDir: File ) extends Actor with ActorLogging {

  val WorkersNumber = 4

  override val supervisorStrategy = OneForOneStrategy() {
    case _ => SupervisorStrategy.Stop
  }

  private def startWorker( file: File ): ActorRef = {
    val worker = context.actorOf( Props(classOf[Worker], connector, file, outputDir.toPath.resolve( file.getName ).toFile ))
    context.watch( worker )
    worker
  }

  override def preStart(): Unit = {
    val (toProcess, rest) = files.splitAt(WorkersNumber)
    val workers = toProcess.map( startWorker )
    context become running( workers.toSet, rest.toList )
  }

  def running( workers: Set[ActorRef], unprocessed : List[File] ) : Receive = {
    case Terminated(actorRef) =>
      val runningWorkers = workers - actorRef
      unprocessed match {
        case first :: rest =>
          context become running( runningWorkers + startWorker(first), rest )

        case Nil if runningWorkers.isEmpty =>
          log.info( "All files processed. Stopping." )
          context.system.shutdown()

        case Nil if runningWorkers.nonEmpty =>
          context become running( runningWorkers, Nil )
      }
  }

  override def receive: Actor.Receive = running( Set.empty, Nil )
}

object RoutingService {
  import play.api.libs.json.Reads
  import scala.concurrent.Future
  import spray.http.Uri
  import spray.http._

  import play.api.libs.json._
  import play.api.libs.json.Reads._
  import play.api.libs.functional.syntax._
  import scala.collection.JavaConversions._

  implicit val geometryReads : Reads[Seq[GeoPos[BigDecimal]]] =
    (__ \ "route_geometry" ).read[String].map(geo => Polyline.decodePolyline(geo, 6))

  implicit val StatusReads : Reads[Status] = ((__ \ "status").read[Int] and (__ \ "status_message").read[String])(Status.apply _)

  implicit val RouteWrites : Reads[Route] = (
    (__ \ "status").read[Int] and
      (__ \ "route_summary" \ "total_distance").read[Int] and
      geometryReads)(Route.apply _)

  case class Status(code: Int, message: String)

  case class Route(status: Int, totalDistance: Int, routeGeometry : Seq[GeoPos[BigDecimal]])
}

object DataEnricher extends App {
  import akka.pattern.ask
  import scala.concurrent.duration._

  implicit val system = akka.actor.ActorSystem()
  import system.dispatcher
  val conf = system.settings.config
  val dataFolder = new java.io.File( conf.getString("data.dir") )
  val feedFiles = if (conf.hasPath("feeds.limit")) dataFolder.listFiles().take( conf.getInt("feeds.limit") ) else dataFolder.listFiles()

  implicit val httpTimeout : Timeout = 3.seconds

  val connector = IO(Http).ask(Http.HostConnectorSetup("boot2docker", port = 9876)).mapTo[Http.HostConnectorInfo].foreach { x =>
    val scheduler = system.actorOf(Props( classOf[JobScheduler], x.hostConnector, feedFiles, new java.io.File("/tmp/output")  ), "workers")
  }

}
