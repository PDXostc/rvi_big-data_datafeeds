package com.advancedtelematic.feed

import org.joda.time.DateTime
import play.api.libs.json.{Json, JsValue, Writes}

case class TraceEntry(id: String, timestamp: DateTime, lat: BigDecimal, lng: BigDecimal, isOccupied: Boolean) {
  def toCsv() : String = s"$id $lat $lng $isOccupied ${timestamp.getMillis / 1000}"
}

object TraceEntry {
  def parse(id: String)(str: String) : TraceEntry = {
    val fields = str.split(" ")
    TraceEntry(
      id = id,
      timestamp = new DateTime( fields(3).toLong * 1000 ),
      lat = BigDecimal( fields(0) ),
      lng = BigDecimal( fields(1) ),
      isOccupied = fields(2) == "1" || fields(2) == "true"
    )
  }

  implicit val TraceAndSpeedWrites = new Writes[(TraceEntry, BigDecimal)] {
    override def writes(value: (TraceEntry, BigDecimal)): JsValue = {
      val (entry, speed) = value
      Json.obj(
        "vin" -> entry.id,
        "timestamp" -> entry.timestamp.getMillis,
        "data" -> Json.arr(
          Json.obj( "channel" -> "location", "value" -> Json.obj( "lat" -> entry.lat, "lon" -> entry.lng ) ),
          Json.obj( "channel" -> "occupancy", "value" -> (if(entry.isOccupied) 1 else 0) ),
          Json.obj( "channel" -> "speed", "value" -> speed)
        )
      )
    }
  }

}
