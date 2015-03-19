package com.advancedtelematic.feed

import org.joda.time.DateTime

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

}
