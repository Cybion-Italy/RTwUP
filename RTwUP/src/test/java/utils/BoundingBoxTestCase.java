package utils;

import org.testng.annotations.Test;
import twitter4j.GeoLocation;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * @author Matteo Moci ( matteo (dot) moci (at) gmail (dot) com )
 */
public class BoundingBoxTestCase {

    @Test
    public void shouldTestBoundingBoxInclusion() {

        double[][] coordinates = new double[2][2];
        coordinates[0][0] = 0.0d;
        coordinates[0][1] = 0.0d;
        coordinates[1][0] = 1.0d;
        coordinates[1][1] = 1.0d;

        final BoundingBox someBb = new BoundingBox(coordinates);

        final GeoLocation placeInsideBB = new GeoLocation(1.0d, 1.0d);
        assertTrue(someBb.includes(placeInsideBB));
        final GeoLocation placeOutsideBB = new GeoLocation(1.0d, 1.1d);
        assertFalse(someBb.includes(placeOutsideBB));
    }

}
