package at.ac.tuwien.aic.streamprocessing.storm.trident.util;

public class Haversine {
    private static final double R = 6372.8; // Earth Radius in kilometers

    /**
     * src https://rosettacode.org/wiki/Haversine_formula#Java
     *
     * @param lat1 start Position
     * @param lon1 start Position
     * @param lat2 end Position
     * @param lon2 end Position
     * @return distance in Kilometers
     */
    public static double calculateDistanceBetween(double lat1, double lon1, double lat2, double lon2) {
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        lat1 = Math.toRadians(lat1);
        lat2 = Math.toRadians(lat2);

        double a = Math.pow(Math.sin(dLat / 2), 2) + Math.pow(Math.sin(dLon / 2), 2) * Math.cos(lat1) * Math.cos(lat2);
        double c = 2 * Math.asin(Math.sqrt(a));
        return R * c;
    }
}
