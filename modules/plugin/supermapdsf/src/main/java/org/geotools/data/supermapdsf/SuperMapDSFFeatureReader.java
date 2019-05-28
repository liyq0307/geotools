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
package org.geotools.data.supermapdsf;

import static org.geotools.data.supermapdsf.SuperMapDSFFileUtils.ID;
import static org.geotools.data.supermapdsf.SuperMapDSFFileUtils.PARTITIONGRID;
import static org.geotools.data.supermapdsf.SuperMapDSFFileUtils.PARTITIONQUADTREE;

import com.alibaba.fastjson.JSONReader;
import java.io.IOException;
import java.io.StringReader;
import java.text.NumberFormat;
import java.util.*;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.geotools.data.FeatureReader;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.util.Converters;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.filter.Filter;
import org.opengis.filter.expression.PropertyName;
import org.opengis.filter.spatial.BBOX;
import org.opengis.geometry.BoundingBox;

public abstract class SuperMapDSFFeatureReader
        implements FeatureReader<SimpleFeatureType, SimpleFeature> {
    String className;

    String fileDirectory = null;

    SimpleFeatureType schema = null;

    int currentIndex = 0;

    private List<Integer> partNumbers = null;

    private List<Boolean> partGeoIsAllBBox = null;

    private Geometry queryGeometry = null;

    private ReferencedEnvelope gridBounds = null;

    private Integer gridRows = 0;

    private Integer gridCols = 0;

    private Double tolerance = 0.0;

    private Integer[] indexMap = null;

    private ArrayList<ReferencedEnvelope> indexRect = null;

    void fromJson(String jsonContext) throws IOException {
        StringReader inputStream = new StringReader(jsonContext);
        JSONReader jsonReader = new JSONReader(inputStream);

        jsonReader.startObject();
        while (jsonReader.hasNext()) {
            String str1 = jsonReader.readString();
            if (str1.compareToIgnoreCase("grid") == 0) {
                jsonReader.startObject();
                Map<String, Object> paris =
                        SuperMapDSFFileUtils.parseReader(
                                jsonReader, SuperMapDSFFileUtils.getCRS(schema));
                gridBounds = (ReferencedEnvelope) paris.get("bounds");
                gridCols = (Integer) paris.get("cols");
                gridRows = (Integer) paris.get("rows");
                tolerance = (double) paris.get("tolerance");
                jsonReader.endObject();
            } else if (str1.compareToIgnoreCase("indexesMap") == 0) {
                indexMap = jsonReader.readObject(Integer[].class);
            } else if (str1.compareToIgnoreCase("indexesRect") == 0) {
                indexRect = new ArrayList<>();
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

    private Geometry getQueryGeometry(BoundingBox bounds) {
        if (null == bounds) {
            return null;
        }

        bounds.setBounds(
                new ReferencedEnvelope(
                        bounds.getMinX() - tolerance,
                        bounds.getMaxX() + tolerance,
                        bounds.getMinY() - tolerance,
                        bounds.getMaxY() + tolerance,
                        null));

        Coordinate[] coords =
                new Coordinate[] {
                    new Coordinate(bounds.getMinX(), bounds.getMinY()),
                    new Coordinate(bounds.getMinX(), bounds.getMaxY()),
                    new Coordinate(bounds.getMaxX(), bounds.getMaxY()),
                    new Coordinate(bounds.getMaxX(), bounds.getMinY()),
                    new Coordinate(bounds.getMinX(), bounds.getMinY())
                };

        return JTSFactoryFinder.getGeometryFactory().createPolygon(coords);
    }

    private boolean initGridPartFile(Filter filter) {
        // 初始化文件编号
        int nCount = gridRows * gridCols;
        partNumbers = new ArrayList<>();
        for (int i = 0; i < nCount; i++) {
            partNumbers.add(i);
        }

        // 根据条件剔除文件编号
        if (filter == Filter.INCLUDE) {
            partGeoIsAllBBox = new ArrayList<>();
            for (int i = 0; i < partNumbers.size(); i++) {
                partGeoIsAllBBox.add(true);
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
            queryGeometry = getQueryGeometry(bounds);

            partGeoIsAllBBox = new ArrayList<>();
            double dUnitX = gridBounds.getWidth() / gridCols;
            double dUnitY = gridBounds.getHeight() / gridRows;

            int nIndex = 0;
            while (nIndex < partNumbers.size()) {
                ReferencedEnvelope indexRect =
                        new ReferencedEnvelope(
                                gridBounds.getMinX()
                                        + (partNumbers.get(nIndex) % gridCols) * dUnitX,
                                gridBounds.getMinX()
                                        + (partNumbers.get(nIndex) % gridCols + 1) * dUnitX,
                                gridBounds.getMaxY()
                                        - (partNumbers.get(nIndex) / gridCols % gridRows + 1)
                                                * dUnitY,
                                gridBounds.getMaxY()
                                        - (partNumbers.get(nIndex) / gridCols % gridRows) * dUnitY,
                                null);

                if (bounds.intersects(indexRect)) {
                    if (bounds.contains(indexRect)) {
                        partGeoIsAllBBox.add(true);
                    } else {
                        partGeoIsAllBBox.add(false);
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

    private boolean initQuadPartFile(Filter filter) throws IOException {
        // 初始化文件编号
        Set<Integer> setFileNumber = new TreeSet<>();
        Collections.addAll(setFileNumber, indexMap);
        partNumbers = new ArrayList<>(setFileNumber);
        if (partNumbers.size() < 1 || partNumbers.get(partNumbers.size() - 1) >= indexRect.size()) {
            return false;
        }

        // 根据条件剔除文件编号
        if (filter == Filter.INCLUDE) {
            partGeoIsAllBBox = new ArrayList<>();
            for (int i = 0; i < partNumbers.size(); i++) {
                partGeoIsAllBBox.add(true);
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
            queryGeometry = getQueryGeometry(bounds);

            partGeoIsAllBBox = new ArrayList<>();
            int nIndex = 0;
            while (nIndex < partNumbers.size()) {
                if (bounds.intersects(indexRect.get(partNumbers.get(nIndex)))) {
                    if (bounds.contains(indexRect.get(partNumbers.get(nIndex)))) {
                        partGeoIsAllBBox.add(true);
                    } else {
                        partGeoIsAllBBox.add(false);
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

    boolean initPartFile(Filter filter) throws IOException {
        if (className.compareToIgnoreCase(PARTITIONGRID) == 0) {
            return initGridPartFile(filter);
        } else if (className.compareToIgnoreCase(PARTITIONQUADTREE) == 0) {
            return initQuadPartFile(filter);
        } else {
            throw new IOException("invalid classname: " + className);
        }
    }

    private boolean isGeometryClass(Class<?> clazz) {
        return clazz == org.locationtech.jts.geom.Point.class
                || clazz == org.locationtech.jts.geom.MultiLineString.class
                || clazz == org.locationtech.jts.geom.MultiPolygon.class;
    }

    SimpleFeature buildFeature(GenericRecord record, Geometry geometry)
            throws IOException, IllegalArgumentException, NoSuchElementException {
        if (null == schema) {
            return null;
        }

        if (null == record) {
            return null;
        }

        SimpleFeatureBuilder builder = new SimpleFeatureBuilder(schema);
        int attributeCount = schema.getAttributeCount();
        for (int i = 0; i < attributeCount; i++) {
            AttributeDescriptor descriptor = schema.getDescriptor(i);
            Class<?> clazz = descriptor.getType().getBinding();
            if (isGeometryClass(clazz)) {
                builder.set(descriptor.getLocalName(), geometry);
            } else {
                Object value = record.get(descriptor.getLocalName());
                builder.add(Converters.convert(value, clazz));
            }
        }

        String fid = record.get(ID).toString();
        if (fid == null || fid.isEmpty()) {
            fid = UUID.randomUUID().toString();
        }

        return builder.buildFeature(fid);
    }

    String getPartFile(String ext) throws IOException {
        if (fileDirectory.isEmpty()) {
            return "";
        }

        while (currentIndex < partNumbers.size()) {
            NumberFormat numfmt = NumberFormat.getInstance(Locale.US);
            numfmt.setMinimumIntegerDigits(6);
            numfmt.setGroupingUsed(false);
            String partFile =
                    fileDirectory + "/part-" + numfmt.format(partNumbers.get(currentIndex)) + ext;

            Path fPath = new Path(partFile);
            Configuration conf = new Configuration();
            FileSystem hdfs = fPath.getFileSystem(conf);

            if (!hdfs.exists(fPath)) {
                currentIndex++;
                continue;
            }

            FileStatus fileStatus = hdfs.getFileStatus(fPath);
            if (fileStatus.getLen() <= 4) {
                currentIndex++;
                continue;
            }

            return partFile;
        }

        return "";
    }

    List<Boolean> getPartGeoIsAllBBox() {
        return partGeoIsAllBBox;
    }

    Geometry getQueryGeometry() {
        return queryGeometry;
    }

    int getIndex(Integer[] newPartNumbers) {
        int index = -1;
        for (int i = 0; i < newPartNumbers.length; i++) {
            if (newPartNumbers[i].equals(partNumbers.get(currentIndex))) {
                index = i;
                break;
            }
        }

        return index;
    }

    boolean isRead(int index, Integer[] newPartNumbers) {
        if (currentIndex > partNumbers.size()) {
            return false;
        }

        if (newPartNumbers == null) {
            return false;
        }

        if (index > 0) {
            for (int i = 0; i < currentIndex; i++) {
                if (newPartNumbers[index - 1].equals(partNumbers.get(i))) {
                    return true;
                }
            }
        }

        return false;
    }

    @Override
    public SimpleFeatureType getFeatureType() {
        return schema;
    }
}
