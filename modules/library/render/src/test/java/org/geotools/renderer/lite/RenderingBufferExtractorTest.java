/*
 *    GeoTools - The Open Source Java GIS Toolkit
 *    http://geotools.org
 *
 *    (C) 2002-2008, Open Source Geospatial Foundation (OSGeo)
 *
 *    This library is free software; you can redistribute it and/or
 *    modify it under the terms of the GNU Lesser General Public
 *    License as published by the Free Software Foundation;
 *    version 2.1 of the License.
 *
 *    This library is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *    Lesser General Public License for more details.
 */
package org.geotools.renderer.lite;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.awt.Color;
import java.io.File;
import java.net.URL;
import org.geotools.api.filter.FilterFactory;
import org.geotools.api.filter.expression.Add;
import org.geotools.api.filter.expression.Function;
import org.geotools.api.style.Graphic;
import org.geotools.api.style.PointPlacement;
import org.geotools.api.style.PointSymbolizer;
import org.geotools.api.style.Rule;
import org.geotools.api.style.Style;
import org.geotools.api.style.Symbolizer;
import org.geotools.api.style.TextSymbolizer;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.filter.function.EnvFunction;
import org.geotools.styling.StyleBuilder;
import org.geotools.test.TestData;
import org.junit.Test;

public class RenderingBufferExtractorTest {
    StyleBuilder sb = new StyleBuilder();
    FilterFactory ff = CommonFactoryFinder.getFilterFactory(null);

    @Test
    public void testTextNoStroke() {
        Style style = sb.createStyle(sb.createTextSymbolizer());
        MetaBufferEstimator rbe = new MetaBufferEstimator();
        assertEquals(0, rbe.getBuffer());
        assertTrue(rbe.isEstimateAccurate());
        rbe.visit(style);
        assertEquals(15, rbe.getBuffer());
        assertTrue(rbe.isEstimateAccurate());
    }

    @Test
    public void testTextDisplaced() {
        TextSymbolizer ts = sb.createTextSymbolizer();
        ts.setFont(sb.createFont("Arial", 20));
        PointPlacement pp = sb.createPointPlacement(1, 1, 10, 10, 0);
        ts.setLabelPlacement(pp);
        Style style = sb.createStyle(ts);
        MetaBufferEstimator rbe = new MetaBufferEstimator();
        rbe.visit(style);
        // 20 (font) + 20 * 0.5 (anchor) + 10 (offset)
        assertEquals(40, rbe.getBuffer());
        assertTrue(rbe.isEstimateAccurate());
    }

    @Test
    public void testSimpleStroke() {
        Style style = sb.createStyle(sb.createLineSymbolizer(sb.createStroke(10.0)));
        MetaBufferEstimator rbe = new MetaBufferEstimator();
        rbe.visit(style);
        assertEquals(10, rbe.getBuffer());
        assertTrue(rbe.isEstimateAccurate());
    }

    @Test
    public void testSimpleGraphic() {
        PointSymbolizer ps = sb.createPointSymbolizer(sb.createGraphic(null, sb.createMark(sb.MARK_CIRCLE), null));
        ps.getGraphic().setSize(sb.literalExpression(15));
        Style style = sb.createStyle(ps);

        MetaBufferEstimator rbe = new MetaBufferEstimator();
        rbe.visit(style);
        assertEquals(16, rbe.getBuffer());
        assertTrue(rbe.isEstimateAccurate());
    }

    @Test
    public void testReachableExternalGraphic() {
        URL resource = TestData.getResource(this, "draw.png");
        PointSymbolizer ps =
                sb.createPointSymbolizer(sb.createGraphic(null, null, sb.createExternalGraphic(resource, "image/png")));
        ps.getGraphic().setSize(sb.literalExpression(null));
        Style style = sb.createStyle(ps);

        MetaBufferEstimator rbe = new MetaBufferEstimator();
        rbe.visit(style);
        assertEquals(24, rbe.getBuffer());
        assertTrue(rbe.isEstimateAccurate());
    }

    @Test
    public void testUnreachableExternalGraphic() throws Exception {
        File file = new File(TestData.getResource(this, "draw.png").toURI());
        URL resource = new File(file.getParent(), "draw-not-there.png").toURI().toURL();
        PointSymbolizer ps =
                sb.createPointSymbolizer(sb.createGraphic(null, null, sb.createExternalGraphic(resource, "image/png")));
        ps.getGraphic().setSize(sb.literalExpression(null));
        Style style = sb.createStyle(ps);

        MetaBufferEstimator rbe = new MetaBufferEstimator();
        rbe.visit(style);
        assertEquals(0, rbe.getBuffer());
        assertFalse(rbe.isEstimateAccurate());
    }

    @Test
    public void testNonIntegerStroke() {
        Style style = sb.createStyle(sb.createLineSymbolizer(sb.createStroke(10.8)));
        MetaBufferEstimator rbe = new MetaBufferEstimator();
        rbe.visit(style);
        assertEquals(11, rbe.getBuffer());
        assertTrue(rbe.isEstimateAccurate());
    }

    @Test
    public void testMultiSymbolizers() {
        Symbolizer ls = sb.createLineSymbolizer(sb.createStroke(10.8));
        Symbolizer ps = sb.createPolygonSymbolizer(sb.createStroke(12), sb.createFill());
        Rule r = sb.createRule(ls, ps);
        MetaBufferEstimator rbe = new MetaBufferEstimator();
        rbe.visit(r);
        assertEquals(12, rbe.getBuffer());
        assertTrue(rbe.isEstimateAccurate());
    }

    @Test
    public void testPropertyWidth() {
        Symbolizer ls = sb.createLineSymbolizer(
                sb.createStroke(sb.colorExpression(Color.BLACK), sb.attributeExpression("gimbo")));
        Symbolizer ps = sb.createPolygonSymbolizer(sb.createStroke(12), sb.createFill());
        Rule r = sb.createRule(ls, ps);
        MetaBufferEstimator rbe = new MetaBufferEstimator();
        rbe.visit(r);
        assertEquals(12, rbe.getBuffer());
        assertFalse(rbe.isEstimateAccurate());
    }

    @Test
    public void testEnvironmentWidth() {
        Symbolizer ls = sb.createLineSymbolizer(sb.createStroke(
                sb.colorExpression(Color.BLACK), ff.function("env", ff.literal("thickness"), ff.literal(10))));
        Rule r = sb.createRule(ls);
        MetaBufferEstimator rbe = new MetaBufferEstimator();

        // no env variable, fall back on the default value
        rbe.visit(r);
        assertEquals(10, rbe.getBuffer());
        assertTrue(rbe.isEstimateAccurate());

        // set the env variable
        EnvFunction.setLocalValue("thickness", 15);
        try {
            rbe.visit(r);
            assertEquals(15, rbe.getBuffer());
            assertTrue(rbe.isEstimateAccurate());
        } finally {
            EnvFunction.clearLocalValues();
        }
    }

    @Test
    public void testConstantFunction() {
        Function cos = ff.function("cos", ff.literal(Math.toRadians(Math.PI)));
        Symbolizer ls = sb.createLineSymbolizer(sb.createStroke(sb.colorExpression(Color.BLACK), cos));
        Rule r = sb.createRule(ls);
        MetaBufferEstimator rbe = new MetaBufferEstimator();

        // cos(pi) == 1
        rbe.visit(r);
        assertEquals(1, rbe.getBuffer());
        assertTrue(rbe.isEstimateAccurate());
    }

    @Test
    public void testMath() {
        Add add = ff.add(ff.literal("5"), ff.literal("-2"));
        Symbolizer ls = sb.createLineSymbolizer(sb.createStroke(sb.colorExpression(Color.BLACK), add));
        Rule r = sb.createRule(ls);
        MetaBufferEstimator rbe = new MetaBufferEstimator();

        // 5-2 = 3
        rbe.visit(r);
        assertEquals(3, rbe.getBuffer());
        assertTrue(rbe.isEstimateAccurate());
    }

    @Test
    public void testLiteralParseStroke() {
        Style style = sb.createStyle(sb.createLineSymbolizer(
                sb.createStroke(sb.colorExpression(Color.BLACK), sb.literalExpression("10.0"))));
        MetaBufferEstimator rbe = new MetaBufferEstimator();
        rbe.visit(style);
        assertEquals(10, rbe.getBuffer());
        assertTrue(rbe.isEstimateAccurate());
    }

    @Test
    public void testNpePreventionStroke() {
        Style style = sb.createStyle(
                sb.createLineSymbolizer(sb.createStroke(sb.colorExpression(Color.BLACK), sb.literalExpression(null))));
        MetaBufferEstimator rbe = new MetaBufferEstimator();
        rbe.visit(style);
        assertEquals(1, rbe.getBuffer());
        assertTrue(rbe.isEstimateAccurate());
    }

    @Test
    public void testLiteralParseGraphics() {
        Graphic g = sb.createGraphic();
        g.setSize(sb.literalExpression("10.0"));
        MetaBufferEstimator rbe = new MetaBufferEstimator();
        rbe.visit(g);
        assertEquals(11, rbe.getBuffer());
        assertTrue(rbe.isEstimateAccurate());
    }
}
