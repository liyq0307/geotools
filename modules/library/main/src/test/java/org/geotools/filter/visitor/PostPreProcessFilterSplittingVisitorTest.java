/*
 *    GeoTools - The Open Source Java GIS Toolkit
 *    http://geotools.org
 *
 *    (C) 2005-2008, Open Source Geospatial Foundation (OSGeo)
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
package org.geotools.filter.visitor;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import org.geotools.api.filter.And;
import org.geotools.api.filter.Filter;
import org.geotools.api.filter.FilterFactory;
import org.geotools.api.filter.Id;
import org.geotools.api.filter.Not;
import org.geotools.api.filter.Or;
import org.geotools.api.filter.PropertyIsBetween;
import org.geotools.api.filter.PropertyIsLike;
import org.geotools.api.filter.PropertyIsNull;
import org.geotools.api.filter.identity.FeatureId;
import org.geotools.api.filter.spatial.BBOX;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.filter.FilterCapabilities;
import org.geotools.filter.function.FilterFunction_geometryType;
import org.geotools.filter.function.JsonPointerFunction;
import org.junit.Before;
import org.junit.Test;

public class PostPreProcessFilterSplittingVisitorTest extends AbstractPostPreProcessFilterSplittingVisitorTests {

    private FilterCapabilities simpleLogicalCaps = new FilterCapabilities();
    private PostPreProcessFilterSplittingVisitor visitor;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        simpleLogicalCaps.addAll(FilterCapabilities.SIMPLE_COMPARISONS_OPENGIS);
        simpleLogicalCaps.addAll(FilterCapabilities.LOGICAL_OPENGIS);
    }

    @Test
    public void testVisitBetweenFilter() throws Exception {
        PropertyIsBetween filter = ff.between(ff.literal(0), ff.property(numAtt), ff.literal(4));
        FilterCapabilities caps = new FilterCapabilities(PropertyIsBetween.class);
        runTest(filter, caps, numAtt);
    }

    @Test
    public void testNullTransactionAccessor() throws Exception {
        accessor = null;
        Filter f1 = createPropertyIsEqualToFilter(nameAtt, "david");
        Filter f2 = createPropertyIsEqualToFilter(nameAtt, "david");

        runTest(ff.and(f1, f2), simpleLogicalCaps, nameAtt);
    }

    @Test
    public void testVisitLogicalANDFilter() throws Exception {
        Filter f1 = createPropertyIsEqualToFilter(nameAtt, "david");
        Filter f2 = createPropertyIsEqualToFilter(nameAtt, "david");

        runTest(ff.and(f1, f2), simpleLogicalCaps, nameAtt);
    }

    @Test
    public void testVisitLogicalNOTFilter() throws Exception {
        Filter f1 = createPropertyIsEqualToFilter(nameAtt, "david");

        runTest(ff.not(f1), simpleLogicalCaps, nameAtt);
    }

    @Test
    public void testVisitLogicalORFilter() throws Exception {
        Filter f1 = createPropertyIsEqualToFilter(nameAtt, "david");
        Filter f2 = createPropertyIsEqualToFilter("name", "jose");

        Filter orFilter = ff.or(f1, f2);
        runTest(orFilter, simpleLogicalCaps, nameAtt);

        visitor = newVisitor(simpleLogicalCaps);

        f2 = ff.bbox(geomAtt, 10.0, 20.0, 10.0, 20.0, "");
        orFilter = ff.or(f1, f2);
        orFilter.accept(visitor, null);

        // f1 could be pre-processed but since f2 can't all the processing has to be done on the
        // client side :-(
        assertEquals(Filter.INCLUDE, visitor.getFilterPre());
        assertEquals(orFilter, visitor.getFilterPost());
    }

    @Test
    public void testMassOrFilter() throws Exception {
        List<Filter> filters = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            filters.add(ff.equals(ff.property(nameAtt), ff.literal("" + i)));
        }
        Or orFilter = ff.or(filters);
        visitor = newVisitor(simpleLogicalCaps);
        // this would have throw the thing into a stack overflow error
        orFilter.accept(visitor, null);
        assertEquals(orFilter, visitor.getFilterPre());
        assertEquals(Filter.INCLUDE, visitor.getFilterPost());
    }

    @Test
    public void testVisitCompareFilter() throws Exception {
        Filter f = createPropertyIsEqualToFilter(nameAtt, "david");

        runTest(f, FilterCapabilities.SIMPLE_COMPARISONS_OPENGIS, nameAtt);
    }

    /** an update is in transaction that modifies an attribute that NOT is referenced in the query */
    @Test
    public void testVisitCompareFilterWithUpdateDifferentAttribute() throws Exception {
        Filter f = createPropertyIsEqualToFilter(nameAtt, "david");

        Filter updateFilter = createPropertyIsEqualToFilter(nameAtt, "jose");

        accessor = new TestAccessor();
        accessor.setUpdate(geomAtt, updateFilter);

        visitor = newVisitor(simpleLogicalCaps);
        f.accept(visitor, null);

        assertEquals(visitor.getFilterPost().toString(), Filter.INCLUDE, visitor.getFilterPost());
        assertEquals(visitor.getFilterPre().toString(), f, visitor.getFilterPre());
    }

    @Test
    public void testVisitLikeFilter() throws Exception {
        Filter filter = ff.like(ff.property(nameAtt), "j*", "*", "?", "\\");
        FilterCapabilities likeCaps = new FilterCapabilities(PropertyIsLike.class);
        runTest(filter, likeCaps, nameAtt);
    }

    @Test
    public void testVisitNullFilter() throws Exception {
        Filter filter = ff.isNull(ff.property(nameAtt));
        FilterCapabilities nullCaps = new FilterCapabilities(PropertyIsNull.class);
        runTest(filter, nullCaps, nameAtt);
    }

    @Test
    public void testVisitFidFilter() throws Exception {
        HashSet<FeatureId> ids = new HashSet<>();
        ids.add(ff.featureId("david"));
        Filter filter = ff.id(ids);
        visitor = newVisitor(new FilterCapabilities(Id.class));
        filter.accept(visitor, null);

        assertEquals(Filter.INCLUDE, visitor.getFilterPost());
        assertEquals(filter, visitor.getFilterPre());
    }

    @Test
    public void testVisitIdFilterWithNoIdCapabilities() throws Exception {
        // Id Filter
        HashSet<FeatureId> ids = new HashSet<>();
        ids.add(ff.featureId("david"));
        Filter idFilter = ff.id(ids);

        // no Id Capabilities
        FilterCapabilities fc = new FilterCapabilities();
        fc.addAll(FilterCapabilities.SIMPLE_COMPARISONS_OPENGIS);
        fc.addType(And.class);

        visitor = newVisitor(fc);
        idFilter.accept(visitor, null);

        assertEquals(Filter.INCLUDE, visitor.getFilterPre());
        assertEquals(idFilter, visitor.getFilterPost());
    }

    @Test
    public void testFunctionFilter() throws Exception {
        simpleLogicalCaps.addType(BBOX.class);
        visitor = newVisitor(simpleLogicalCaps);

        Filter filter = createFunctionFilter();

        filter.accept(visitor, null);

        assertEquals(filter, visitor.getFilterPost());
        assertEquals(Filter.INCLUDE, visitor.getFilterPre());

        simpleLogicalCaps.addType(FilterFunction_geometryType.class);
        visitor = newVisitor(simpleLogicalCaps);

        filter.accept(visitor, null);

        assertEquals(Filter.INCLUDE, visitor.getFilterPost());
        assertEquals(filter, visitor.getFilterPre());
    }

    @Test
    public void testFunctionANDGeometryFilter() throws Exception {
        simpleLogicalCaps.addType(BBOX.class);
        visitor = newVisitor(simpleLogicalCaps);

        Filter funtionFilter = createFunctionFilter();
        Filter geomFilter = ff.bbox(geomAtt, 10, 20, 10, 20, "");

        Filter andFilter = ff.and(funtionFilter, geomFilter);

        andFilter.accept(visitor, null);

        assertEquals(funtionFilter.toString(), visitor.getFilterPost().toString());
        assertEquals(geomFilter.toString(), visitor.getFilterPre().toString());

        simpleLogicalCaps.addType(FilterFunction_geometryType.class);
        visitor = newVisitor(simpleLogicalCaps);

        andFilter.accept(visitor, null);

        assertEquals(Filter.INCLUDE, visitor.getFilterPost());
        assertEquals(andFilter, visitor.getFilterPre());
    }

    @Test
    public void testFunctionORGeometryFilter() throws Exception {
        simpleLogicalCaps.addType(BBOX.class);
        visitor = newVisitor(simpleLogicalCaps);

        Filter funtionFilter = createFunctionFilter();
        Filter geomFilter = ff.bbox(geomAtt, 10, 20, 10, 20, "");

        Filter orFilter = ff.or(funtionFilter, geomFilter);

        orFilter.accept(visitor, null);

        assertEquals(Filter.INCLUDE, visitor.getFilterPre());
        assertEquals(orFilter, visitor.getFilterPost());

        simpleLogicalCaps.addType(FilterFunction_geometryType.class);
        visitor = newVisitor(simpleLogicalCaps);

        orFilter.accept(visitor, null);

        assertEquals(Filter.INCLUDE, visitor.getFilterPost());
        assertEquals(orFilter, visitor.getFilterPre());
    }

    @Test
    public void testFunctionNOTFilter() throws Exception {
        simpleLogicalCaps.addType(BBOX.class);
        visitor = newVisitor(simpleLogicalCaps);

        Filter funtionFilter = createFunctionFilter();

        Filter not = ff.not(funtionFilter);
        not.accept(visitor, null);

        assertEquals(not, visitor.getFilterPost());
        assertEquals(Filter.INCLUDE, visitor.getFilterPre());

        simpleLogicalCaps.addType(FilterFunction_geometryType.class);
        visitor = newVisitor(simpleLogicalCaps);

        not.accept(visitor, null);

        assertEquals(Filter.INCLUDE, visitor.getFilterPost());
        assertEquals(not, visitor.getFilterPre());
    }

    @Test
    public void testNullParentNullAccessor() throws Exception {
        simpleLogicalCaps.addType(BBOX.class);
        simpleLogicalCaps.addType(FilterFunction_geometryType.class);
        visitor = newVisitor(simpleLogicalCaps);

        Filter funtionFilter = createFunctionFilter();
        Filter geomFilter = ff.bbox(geomAtt, 10.0, 20.0, 10.0, 20.0, "");

        Filter orFilter = ff.or(funtionFilter, geomFilter);
        visitor = new PostPreProcessFilterSplittingVisitor(new FilterCapabilities(), null, null);
        orFilter.accept(visitor, null);

        assertEquals(Filter.INCLUDE, visitor.getFilterPre());
        assertEquals(orFilter, visitor.getFilterPost());

        visitor = new PostPreProcessFilterSplittingVisitor(simpleLogicalCaps, null, null);

        orFilter.accept(visitor, null);

        assertEquals(Filter.INCLUDE, visitor.getFilterPost());
        assertEquals(orFilter, visitor.getFilterPre());
    }

    @Test
    public void testComplicatedOrFilter() throws Exception {
        Filter c1 = ff.equals(ff.property("eventstatus"), ff.literal("deleted"));

        Filter c2 = ff.equals(ff.property("eventtype"), ff.literal("road hazard"));

        Filter c3 = ff.equals(ff.property("eventtype"), ff.literal("area warning"));

        Filter g = ff.bbox("geom", 0, 180, 0, 90, "");

        Filter f = ff.or(c2, c3);
        f = ff.and(ff.not(c1), f);
        f = ff.and(f, g);

        simpleLogicalCaps.addType(BBOX.class);
        simpleLogicalCaps.addType(FilterFunction_geometryType.class);

        visitor = new PostPreProcessFilterSplittingVisitor(simpleLogicalCaps, null, null);
        f.accept(visitor, null);

        assertEquals(f, visitor.getFilterPre());
        assertEquals(Filter.INCLUDE, visitor.getFilterPost());

        visitor = new PostPreProcessFilterSplittingVisitor(simpleLogicalCaps, null, new ClientTransactionAccessor() {

            @Override
            public Filter getDeleteFilter() {
                return null;
            }

            @Override
            public Filter getUpdateFilter(String attributePath) {
                if (attributePath.equals("eventtype")) {
                    HashSet<FeatureId> ids = new HashSet<>();
                    ids.add(ff.featureId("fid"));
                    return ff.id(ids);
                }
                return null;
            }
        });

        f.accept(visitor, null);

        HashSet<FeatureId> ids = new HashSet<>();
        ids.add(ff.featureId("fid"));

        assertEquals(f, visitor.getFilterPost());
        assertEquals(ff.or(f, ff.id(ids)), visitor.getFilterPre());
    }

    @Test
    public void testOrNotSupported() {
        FilterCapabilities caps = new FilterCapabilities();
        caps.addAll(FilterCapabilities.SIMPLE_COMPARISONS_OPENGIS);
        caps.addType(And.class);
        PostPreProcessFilterSplittingVisitor visitor = new PostPreProcessFilterSplittingVisitor(caps, null, null);

        FilterFactory ff = CommonFactoryFinder.getFilterFactory(null);
        Filter f1 = ff.equals(ff.property("CFCC"), ff.literal("A41"));
        Filter f2 = ff.equals(ff.property("CFCC"), ff.literal("A42"));

        Filter ored = ff.or(f1, f2);
        ored.accept(visitor, null);
        assertEquals(Filter.INCLUDE, visitor.getFilterPre());
        assertEquals(ored, visitor.getFilterPost());

        Filter anded = ff.and(f1, f2);
        visitor = new PostPreProcessFilterSplittingVisitor(caps, null, null);
        anded.accept(visitor, null);
        assertEquals(anded, visitor.getFilterPre());
        assertEquals(Filter.INCLUDE, visitor.getFilterPost());
    }

    @Test
    public void testTemporalFilter() throws Exception {
        FilterCapabilities caps = new FilterCapabilities();
        PostPreProcessFilterSplittingVisitor visitor = new PostPreProcessFilterSplittingVisitor(caps, null, null);

        Filter f1 = ff.after(ff.property("foo"), ff.literal("2011-06-20"));
        f1.accept(visitor, null);

        assertEquals(f1, visitor.getFilterPost());
    }

    @Test
    public void testIsNullFilter() {
        FilterCapabilities caps = new FilterCapabilities();
        PostPreProcessFilterSplittingVisitor visitor = new PostPreProcessFilterSplittingVisitor(caps, null, null);
        caps.addType(PropertyIsNull.class);

        Filter f1 = ff.isNull(ff.literal("abc"));
        f1.accept(visitor, null);

        assertEquals(f1, visitor.getFilterPre());

        visitor = new PostPreProcessFilterSplittingVisitor(caps, null, null);
        Filter f2 = ff.isNull(ff.function("strConcat", ff.literal("abc"), ff.literal("def")));
        f2.accept(visitor, null);

        assertEquals(f2, visitor.getFilterPost());
    }

    @Test
    public void testNullLiteralInLogicCombination() {
        FilterCapabilities caps = new FilterCapabilities();
        PostPreProcessFilterSplittingVisitor visitor = new PostPreProcessFilterSplittingVisitor(caps, null, null);
        caps.addAll(FilterCapabilities.SIMPLE_COMPARISONS_OPENGIS);
        caps.addType(And.class);
        caps.addType(Or.class);
        caps.addType(Not.class);

        Filter f1 = ff.equal(ff.literal(null), ff.literal("test"), false);
        Filter f2 = ff.not(ff.equal(ff.literal(null), ff.literal("test"), false));
        Filter or = ff.or(Arrays.asList(f1, f2));
        or.accept(visitor, null);
        assertEquals(or, visitor.getFilterPre());
        assertEquals(Filter.INCLUDE, visitor.getFilterPost());
    }

    @Test
    public void testIndexOutOfBoundExceptionNotHappensWithTwoInMemoryFilter() {
        // test a bug doesn't occur when having in the filter a function
        // with a number of parameters <= the number of filters in the post stack
        FilterCapabilities caps = new FilterCapabilities();
        PostPreProcessFilterSplittingVisitor visitor = new PostPreProcessFilterSplittingVisitor(caps, null, null);
        caps.addAll(FilterCapabilities.SIMPLE_COMPARISONS_OPENGIS);
        caps.addType(JsonPointerFunction.class);
        caps.addType(And.class);
        Filter memoryOne = ff.crosses((String) null, null);
        Filter memoryTwo = ff.crosses((String) null, null);
        Filter otherFilter = ff.equal(
                ff.function("jsonPointer", ff.literal("/pointer"), ff.property("property")), ff.literal("test"), false);
        And and = ff.and(Arrays.asList(memoryOne, memoryTwo, otherFilter));
        and.accept(visitor, null);
        assertEquals(ff.and(Arrays.asList(memoryOne, memoryTwo)), visitor.getFilterPost());
        assertEquals(otherFilter, visitor.getFilterPre());
    }
}
