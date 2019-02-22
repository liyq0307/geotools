/*
 *    GeoTools - The Open Source Java GIS Toolkit
 *    http://geotools.org
 *
 *    (C) 2002-2011, Open Source Geospatial Foundation (OSGeo)
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
package org.geotools.data.bdtindexfile;

import java.io.IOException;
import java.util.NoSuchElementException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.locationtech.jts.io.WKBReader;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

class QuadAvroFeatureReader extends QuadFeatureReader {

    public QuadAvroFeatureReader(SimpleFeatureType sft, String fileDy) {
        schema = sft;
        fileDirectory = fileDy;
        featureBuilder = new SimpleFeatureBuilder(sft);
        wkbReader = new WKBReader();
    }

    @Override
    public SimpleFeature next()
            throws IOException, IllegalArgumentException, NoSuchElementException {
        if (hasNext()) {
            return avroNext();
        }
        return null;
    }

    @Override
    public boolean hasNext() throws IOException {
        return avroHasNext();
    }

    @Override
    public void close() throws IOException {}
}
