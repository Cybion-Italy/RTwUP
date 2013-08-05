package utils;

import twitter4j.GeoLocation;

public class BoundingBox {

    private final double[] southWest;

    private final double[] northEast;

    private final double[][] coordinates;

    public BoundingBox(double[][] coordinates) {
        this.coordinates = coordinates;
        this.southWest = coordinates[0];
        this.northEast = coordinates[1];
    }

    public boolean includes(GeoLocation gl) {

        double latitude = gl.getLatitude();
        double longitude = gl.getLongitude();

        if ((latitude >= this.southWest[1] && latitude <= this.northEast[1]) &&
            (longitude >= this.southWest[0] && longitude <= this.northEast[0])) {
            return true;
        }
        return false;
    }

    public double[][] getCoordinates() {

        return coordinates;
    }
}
