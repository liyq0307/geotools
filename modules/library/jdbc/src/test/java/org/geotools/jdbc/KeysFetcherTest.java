/*
 *    GeoTools - The Open Source Java GIS Toolkit
 *    http://geotools.org
 *
 *    (C) 2015, Open Source Geospatial Foundation (OSGeo)
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
package org.geotools.jdbc;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import org.junit.Assert;
import org.junit.Test;

public class KeysFetcherTest {
    @Test
    public void testIncrement() throws IOException {
        Assert.assertEquals(Short.valueOf((short) 4), KeysFetcher.FromPreviousIntegral.increment((short) 3));
        Assert.assertEquals(Integer.valueOf(11), KeysFetcher.FromPreviousIntegral.increment(10));
        Assert.assertEquals(Long.valueOf(21), KeysFetcher.FromPreviousIntegral.increment(20L));
        Assert.assertEquals(BigInteger.valueOf(31), KeysFetcher.FromPreviousIntegral.increment(BigInteger.valueOf(30)));
        Assert.assertEquals(BigDecimal.valueOf(41), KeysFetcher.FromPreviousIntegral.increment(BigDecimal.valueOf(40)));

        try {
            KeysFetcher.FromPreviousIntegral.increment("boom");
        } catch (IOException e) {
            // expected
        }
    }
}
