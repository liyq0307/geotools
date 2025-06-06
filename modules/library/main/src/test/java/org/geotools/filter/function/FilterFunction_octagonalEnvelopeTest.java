package org.geotools.filter.function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.filter.FilterFactory;
import org.geotools.api.filter.expression.Function;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.util.factory.GeoTools;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.OctagonalEnvelope;
import org.locationtech.jts.geom.Polygon;

/**
 * The FilterFunction_octagonalEnvelope UnitTest
 *
 * @author Jared Erickson
 */
public class FilterFunction_octagonalEnvelopeTest {

    /** Test of getArgCount method, of class FilterFunction_octagonalEnvelope. */
    @Test
    public void testGetArgCount() {
        FilterFunction_octagonalEnvelope f = new FilterFunction_octagonalEnvelope();
        assertEquals(1, f.getFunctionName().getArgumentCount());
    }

    /** Test of getName method, of class FilterFunction_octagonalEnvelope. */
    @Test
    public void getName() {
        FilterFunction_octagonalEnvelope f = new FilterFunction_octagonalEnvelope();
        assertEquals("octagonalenvelope", f.getName());
    }

    /** Test of evaluate method, of class FilterFunction_octagonalEnvelope. */
    @Test
    public void testEvaluate() throws Exception {
        SimpleFeatureCollection featureCollection = FunctionTestFixture.polygons();

        // Test the Function
        FilterFactory ff = CommonFactoryFinder.getFilterFactory(GeoTools.getDefaultHints());
        Function exp = ff.function("octagonalenvelope", ff.property("geom"));
        try (SimpleFeatureIterator iter = featureCollection.features()) {
            while (iter.hasNext()) {
                SimpleFeature feature = iter.next();
                Geometry geom = (Geometry) feature.getDefaultGeometry();
                Geometry octagonalEnvelope = new OctagonalEnvelope(geom).toGeometry(geom.getFactory());
                Object value = exp.evaluate(feature);
                assertTrue(value instanceof Polygon);
                assertTrue(octagonalEnvelope.equalsExact((Geometry) value, 0.1));
            }
        }

        // Check for null safeness
        assertNull(exp.evaluate(null));
    }
}
