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
package org.geotools.data.supermapindexfile;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.NumberFormat;
import java.util.*;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.geotools.data.FeatureReader;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;

import static org.geotools.data.supermapindexfile.SuperMapIndexFileUtils.*;

public abstract class SuperMapIndexFileFeatureReader implements FeatureReader<SimpleFeatureType, SimpleFeature> {

    SimpleFeatureType schema = null;

    String fileDirectory = null;

    ReferencedEnvelope gridBounds = null;

    Integer gridRows = 0;

    Integer gridCols = 0;

    Double gridTolerance = 0.0;

    List<Integer> partNumbers = null;

    List<Boolean> partGeoIsAllBbox = null;

    Geometry queryGeometry = null;

    SimpleFeatureBuilder featureBuilder = null;

    WKBReader wkbReader = null;

    private int currentFileIndex = 0;

    // Avro使用
    private FileReader<GenericRecord> avroReader = null;

    private boolean bIsUseRecord = true;
    private GenericRecord record = null;
    private Geometry reGeometry = null;

    public abstract void fromJson(String jsonContext) throws IOException;

    public abstract boolean initPartFile(Filter filter) throws IOException;

    @Override
    public SimpleFeatureType getFeatureType() {
        return schema;
    }

    public SimpleFeature avroNext()
            throws IOException, IllegalArgumentException, NoSuchElementException {
        featureBuilder.reset();
        int attributeCount = schema.getAttributeCount();
        for (int i = 0; i < attributeCount; i++) {
            String fieldName = schema.getDescriptor(i).getLocalName();
            Class fieldType = schema.getDescriptor(i).getType().getBinding();

            if (fieldType == Integer.class
                    || fieldType == Long.class
                    || fieldType == Double.class
                    || fieldType == Float.class
                    || fieldType == String.class
                    || fieldType == Boolean.class
                    || fieldType == ByteBuffer.class) {
                featureBuilder.set(fieldName, record.get(fieldName));
            } else if (fieldType == Date.class) {
                Object olDate = record.get(fieldName);
                if (olDate != null) {
                    Date dateValue = new Date((Long) olDate);
                    featureBuilder.set(fieldName, dateValue);
                } else {
                    featureBuilder.set(fieldName, olDate);
                }
            } else if (fieldType == Point.class
                    || fieldType == MultiLineString.class
                    || fieldType == MultiPolygon.class) {
                featureBuilder.set(fieldName, reGeometry);
            }
        }

        String fid = record.get(ID).toString();
        if (fid == null || fid.isEmpty()) {
            fid = java.util.UUID.randomUUID().toString();
        }

        bIsUseRecord = true;
        return featureBuilder.buildFeature(fid);
    }

    public boolean avroHasNext() throws IOException {
        while (bIsUseRecord) {
            // 使用了需要重新获取
            if (avroReader != null) {
                if (avroReader.hasNext()) {
                    // 存在
                    record = avroReader.next(record);
                    String[] tiles = record.get(TILES).toString().split(",");
                    if (tiles != null && tiles.length > 1) {
                        Set<Integer> setTiles = new TreeSet<>();
                        for (String tile : tiles) {
                            setTiles.add(Integer.parseInt(tile));
                        }
                        Integer[] geometryPartNumbers = setTiles.toArray(new Integer[setTiles.size()]);

                        int nIndex = -1;
                        for (int i = 0; i < geometryPartNumbers.length; i++) {
                            if (geometryPartNumbers[i].equals(partNumbers.get(currentFileIndex))) {
                                nIndex = i;
                                break;
                            }
                        }

                        boolean bIsRead = false;
                        for (int i = 0; i < nIndex; i++) {
                            for (int j = 0; j < currentFileIndex; j++) {
                                if (geometryPartNumbers[i].equals(partNumbers.get(j))) {
                                    bIsRead = true;
                                    break;
                                }
                            }
                            if (bIsRead) {
                                break;
                            }
                        }
                        if (bIsRead) {
                            continue;
                        }
                    }

                    // 取几何对象
                    Class geoType = schema.getGeometryDescriptor().getType().getBinding();
                    if (geoType == Point.class) {
                        Double dX = Double.parseDouble(record.get(X).toString());
                        Double dY = Double.parseDouble(record.get(Y).toString());
                        reGeometry = JTSFactoryFinder.getGeometryFactory().createPoint(new Coordinate(dX, dY));
                    } else if (geoType == MultiLineString.class || geoType == MultiPolygon.class) {
                        try {
                            reGeometry = wkbReader.read(((ByteBuffer) record.get(GEOMETRY)).array());
                        } catch (ParseException e) {
                            throw new IOException("Unknown WKB");
                        }
                    } else {
                        continue;
                    }

                    if (!partGeoIsAllBbox.get(currentFileIndex) && !queryGeometry.intersects(reGeometry)) {
                        continue;
                    }

                    bIsUseRecord = false;
                    continue;
                } else {
                    avroReader.close();
                    currentFileIndex++;
                }
            }

            String partFile = getAvroPartFile();
            if (partFile.isEmpty()) {
                avroReader = null;
                return false;
            }

            Configuration conf = new Configuration();
            FsInput fsInput = new FsInput(new Path(partFile), conf);
            avroReader = DataFileReader.openReader(fsInput, new GenericDatumReader<>());
        }
        return true;
    }

    private String getAvroPartFile() throws IOException {
        while (currentFileIndex < partNumbers.size()) {
            NumberFormat numfmt = NumberFormat.getInstance(Locale.US);
            numfmt.setMinimumIntegerDigits(6);
            numfmt.setGroupingUsed(false);
            String partFile = fileDirectory + "/part-" + numfmt.format(partNumbers.get(currentFileIndex)) + ".avro";

            Path fPath = new Path(partFile);
            Configuration conf = new Configuration();
            FileSystem hdfs = fPath.getFileSystem(conf);

            if (!hdfs.exists(fPath)) {
                currentFileIndex++;
                continue;
            }

            FileStatus fileStatus = hdfs.getFileStatus(fPath);
            if (fileStatus.getLen() <= 4) {
                currentFileIndex++;
                continue;
            }

            return partFile;
        }
        return "";
    }
}
