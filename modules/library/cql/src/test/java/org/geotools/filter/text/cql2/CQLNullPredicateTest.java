/*
 *    GeoTools - The Open Source Java GIS Toolkit
 *    http://geotools.org
 *
 *    (C) 2004-2008, Open Source Geospatial Foundation (OSGeo)
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

package org.geotools.filter.text.cql2;

import org.geotools.api.filter.Filter;
import org.geotools.filter.text.commons.CompilerUtil;
import org.geotools.filter.text.commons.Language;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test CQL Null Predicate:
 *
 * <p>
 *
 * <pre>
 * &lt;null predicate &gt; ::=  &lt;attribute name &gt; IS [ NOT ] NULL
 * </pre>
 *
 * @author Mauricio Pazos (Axios Engineering)
 * @since 2.5
 */
public class CQLNullPredicateTest {
    protected final Language language;

    public CQLNullPredicateTest() {
        this(Language.CQL);
    }

    protected CQLNullPredicateTest(final Language language) {

        assert language != null : "language cannot be null";

        this.language = language;
    }

    /** test the */
    protected void testNullPredicate(final String samplePredicate, Filter expected) throws Exception {

        Filter actual = parseFilter(samplePredicate);

        Assert.assertNotNull("expects a not null filter", actual);

        Assert.assertEquals("Filter error", expected, actual);
    }

    protected Filter parseFilter(String samplePredicate) throws CQLException {
        return CompilerUtil.parseFilter(this.language, samplePredicate);
    }

    /** Sample: ATTR1 IS NULL */
    @Test
    public void propertyIsNull() throws Exception {

        final String samplePredicate = FilterCQLSample.PROPERTY_IS_NULL;
        Filter expected = FilterCQLSample.getSample(samplePredicate);
        testNullPredicate(samplePredicate, expected);
    }

    /** Sample: ATTR1 IS NOT NULL */
    @Test
    public void propertyIsNotNull() throws Exception {

        final String samplePredicate = FilterCQLSample.PROPERTY_IS_NOT_NULL;
        Filter expected = FilterCQLSample.getSample(samplePredicate);
        testNullPredicate(samplePredicate, expected);
    }
}
