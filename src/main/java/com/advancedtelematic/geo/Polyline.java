package com.advancedtelematic.geo;

import java.util.ArrayList;
import java.util.List;
import scala.math.BigDecimal;

/**
 * Created by vladimir on 25/02/15.
 */
public class Polyline {

    public static List<GeoPos<BigDecimal>> decodePolyline(String encoded, int precision) {
        List<GeoPos<BigDecimal>> poly = new ArrayList<>();

        int index = 0, len = encoded.length();
        int lat = 0, lng = 0;
        while (index < len) {
            int b, shift = 0, result = 0;
            do {
                b = encoded.charAt(index++) - 63;
                result |= (b & 0x1f) << shift;
                shift += 5;
            } while (b >= 0x20);
            int dlat = ((result & 1) != 0 ? ~(result >> 1) : (result >> 1));
            lat += dlat;
            shift = 0;
            result = 0;
            do {
                b = encoded.charAt(index++) - 63;
                result |= (b & 0x1f) << shift;
                shift += 5;
            } while (b >= 0x20);
            int dlng = ((result & 1) != 0 ? ~(result >> 1) : (result >> 1));
            lng += dlng;


            poly.add(GeoPos.create(lat, lng, precision));
        }
        return poly;
    }

    public static void main(String[] args) {
        System.out.println(decodePolyline("q`c_gAnrlmhFvZqAxB_@rCeAfBc@xU{@_C{bATmJL_KYuPc@gKmCLmt@vBaDQkHuA", 6));
    }
}
