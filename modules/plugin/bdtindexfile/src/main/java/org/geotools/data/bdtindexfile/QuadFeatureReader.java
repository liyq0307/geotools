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

import com.alibaba.fastjson.JSONReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Set;
import java.util.TreeSet;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.locationtech.jts.geom.Coordinate;
import org.opengis.filter.Filter;
import org.opengis.filter.expression.PropertyName;
import org.opengis.filter.spatial.BBOX;
import org.opengis.geometry.BoundingBox;

public abstract class QuadFeatureReader extends BdtIndexFileFeatureReader {

    private Integer[] indexMap = null;

    private ArrayList<ReferencedEnvelope> indexRect = null;

    @Override
    public void fromJson(String jsonContext) throws IOException {
        StringReader inputStream = new StringReader(jsonContext);
        JSONReader jsonReader = new JSONReader(inputStream);

        jsonReader.startObject();
        while (jsonReader.hasNext()) {
            String str1 = jsonReader.readString();
            if (str1.compareToIgnoreCase("grid") == 0) {
                jsonReader.startObject();
                while (jsonReader.hasNext()) {
                    String str2 = jsonReader.readString();
                    if (str2.compareToIgnoreCase("bounds") == 0) {
                        jsonReader.startArray();
                        double minx = jsonReader.readObject(Double.class);
                        double maxy = jsonReader.readObject(Double.class);
                        double maxx = jsonReader.readObject(Double.class);
                        double miny = jsonReader.readObject(Double.class);
                        jsonReader.endArray();
                        gridBounds = new ReferencedEnvelope(minx, maxx, miny, maxy, null);
                    } else if (str2.compareToIgnoreCase("rows") == 0) {
                        gridRows = jsonReader.readInteger();
                    } else if (str2.compareToIgnoreCase("cols") == 0) {
                        gridCols = jsonReader.readInteger();
                    } else if (str2.compareToIgnoreCase("tolerance") == 0) {
                        gridTolerance = jsonReader.readObject(Double.class);
                    } else {
                        throw new IOException("invalid key: " + str2);
                    }
                }
                jsonReader.endObject();
            } else if (str1.compareToIgnoreCase("indexesMap") == 0) {
                indexMap = jsonReader.readObject(Integer[].class);
            } else if (str1.compareToIgnoreCase("indexesRect") == 0) {
                indexRect = new ArrayList<ReferencedEnvelope>();
                jsonReader.startArray();
                while (jsonReader.hasNext()) {
                    jsonReader.startArray();
                    double minx = jsonReader.readObject(Double.class);
                    double maxy = jsonReader.readObject(Double.class);
                    double maxx = jsonReader.readObject(Double.class);
                    double miny = jsonReader.readObject(Double.class);
                    jsonReader.endArray();
                    indexRect.add(new ReferencedEnvelope(minx, maxx, miny, maxy, null));
                }
                jsonReader.endArray();
            } else {
                throw new IOException("invalid key: " + str1);
            }
        }
        jsonReader.endObject();
        jsonReader.close();
        inputStream.close();
    }

    @Override
    public boolean initPartFile(Filter filter) throws IOException {
        // 初始化文件编号
        Set<Integer> setFileNumber = new TreeSet<Integer>();
        for (Integer nFileNumber : indexMap) {
            setFileNumber.add(nFileNumber);
        }
        partNumbers = new ArrayList<>(setFileNumber);
        if (partNumbers.size() < 1 || partNumbers.get(partNumbers.size() - 1) >= indexRect.size()) {
            return false;
        }

        // 根据条件剔除文件编号
        if (filter == Filter.INCLUDE) {
            partGeoIsAllBbox = new ArrayList<Boolean>();
            for (int i = 0; i < partNumbers.size(); i++) {
                partGeoIsAllBbox.add(true);
            }
            return true;
        } else if (filter instanceof BBOX) {
            BBOX bbox = (BBOX) filter;
            if (bbox.getExpression1() instanceof PropertyName) {
                if (!((PropertyName) bbox.getExpression1())
                        .getPropertyName()
                        .equals(schema.getGeometryDescriptor().getLocalName())) {
                    return false;
                }
            } else {
                return false;
            }

            BoundingBox bounds = bbox.getBounds();
            bounds.setBounds(
                    new ReferencedEnvelope(
                            bounds.getMinX() - gridTolerance,
                            bounds.getMaxX() + gridTolerance,
                            bounds.getMinY() - gridTolerance,
                            bounds.getMaxY() + gridTolerance,
                            null));

            Coordinate[] coords =
                    new Coordinate[] {
                        new Coordinate(bounds.getMinX(), bounds.getMinY()),
                        new Coordinate(bounds.getMinX(), bounds.getMaxY()),
                        new Coordinate(bounds.getMaxX(), bounds.getMaxY()),
                        new Coordinate(bounds.getMaxX(), bounds.getMinY()),
                        new Coordinate(bounds.getMinX(), bounds.getMinY())
                    };
            queryGeometry = JTSFactoryFinder.getGeometryFactory().createPolygon(coords);

            partGeoIsAllBbox = new ArrayList<Boolean>();
            int nIndex = 0;
            while (nIndex < partNumbers.size()) {
                if (bounds.intersects(indexRect.get(partNumbers.get(nIndex)))) {
                    if (bounds.contains(indexRect.get(partNumbers.get(nIndex)))) {
                        partGeoIsAllBbox.add(true);
                    } else {
                        partGeoIsAllBbox.add(false);
                    }
                    nIndex++;
                } else {
                    partNumbers.remove(nIndex);
                }
            }

            return true;
        }

        return false;
    }
}
