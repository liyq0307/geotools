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

import static org.geotools.data.supermapdsf.SuperMapDSFUtils.*;

import com.alibaba.fastjson.JSONReader;
import java.io.IOException;
import java.io.StringReader;
import java.text.NumberFormat;
import java.util.*;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.geotools.data.FeatureReader;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.util.Converters;
import org.locationtech.jts.geom.*;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

public abstract class SuperMapDSFFeatureReader
        implements FeatureReader<SimpleFeatureType, SimpleFeature> {
    SimpleFeatureType schema = null;

    int currentIndex = -1;

    private String className;

    private String fileDirectory = null;

    private String compress = "UnCompressed";

    private Set<String> deleteFeatures = new TreeSet<>();

    private Map<String, Long> modifiedFeatures = new HashMap<>();

    private List<Integer> partNumbers = null;

    private Integer gridRows = 0;

    private Integer gridCols = 0;

    private Double tolerance = 0.0;

    private Integer[] indexMap = null;

    private ArrayList<ReferencedEnvelope> indexRect = null;

    void setClassName(String className) {
        this.className = className;
    }

    void setFileDirectory(String fileDirectory) {
        this.fileDirectory = fileDirectory;
    }

    String getCompress() {
        return compress;
    }

    void setCompress(String compress) {
        this.compress = compress;
    }

    Double getTolerance() {
        return this.tolerance;
    }

    void fromJson(String jsonContext) throws IOException {
        StringReader inputStream = new StringReader(jsonContext);
        JSONReader jsonReader = new JSONReader(inputStream);

        jsonReader.startObject();
        while (jsonReader.hasNext()) {
            String str1 = jsonReader.readString();
            if (str1.compareToIgnoreCase("grid") == 0) {
                jsonReader.startObject();
                Map<String, Object> paris =
                        SuperMapDSFUtils.parseReader(jsonReader, SuperMapDSFUtils.getCRS(schema));
                gridCols = (Integer) paris.get("cols");
                gridRows = (Integer) paris.get("rows");
                tolerance = (Double) paris.get("tolerance");
                jsonReader.endObject();
            } else if (str1.compareToIgnoreCase("indexesMap") == 0) {
                Integer[] values = jsonReader.readObject(Integer[].class);
                indexMap = unzipArray(values);
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

    /**
     * 解压缩IndexMap
     *
     * @param values 数组
     * @return 解压缩的数据
     */
    private Integer[] unzipArray(Integer[] values) {
        if (values.length > 0 && values[0] == -1234543210) {
            List<Integer> temValues = new ArrayList<>();
            for (int i = 1; i < values.length; i += 2) {
                int count = values[i + 1];
                Integer[] temp = new Integer[count];
                Arrays.fill(temp, values[i]);
                temValues.addAll(Arrays.asList(temp));
            }

            Integer[] newValues = new Integer[temValues.size()];
            return temValues.toArray(newValues);
        } else {
            return values;
        }
    }

    void readDeleteFeatures() throws IOException {
        String partRoot = getPartitionRoot(new Configuration());
        Path delFile = new Path(partRoot + "/_delete.txt");
        Configuration conf = new Configuration();
        FileSystem fs = delFile.getFileSystem(conf);
        if (fs.exists(delFile)) {
            FSDataInputStream inputStream = fs.open(delFile);
            List<String> lines = IOUtils.readLines(inputStream, "UTF-8");
            lines.forEach(
                    line -> {
                        String[] tokens = line.split(",");
                        if (tokens.length == 3) {
                            if (Integer.parseInt(tokens[1]) == 1) {
                                deleteFeatures.add(tokens[0]);
                            } else if (Integer.parseInt(tokens[1]) == 2) {
                                Long lastTime = Long.parseLong(tokens[2]);
                                modifiedFeatures.put(tokens[0], lastTime);
                            }
                        }
                    });
        }
    }

    boolean initPartFile() throws IOException {
        if (className.compareToIgnoreCase(PARTITIONGRID) == 0) {
            // 初始化文件编号
            int nCount = gridRows * gridCols;
            partNumbers = new ArrayList<>();
            for (int i = 0; i < nCount; i++) {
                partNumbers.add(i);
            }

            return partNumbers.size() >= 1;
        } else if (className.compareToIgnoreCase(PARTITIONQUADTREE) == 0) {
            // 初始化文件编号
            Set<Integer> setFileNumber = new TreeSet<>();
            Collections.addAll(setFileNumber, indexMap);
            partNumbers = new ArrayList<>(setFileNumber);
            return partNumbers.size() >= 1
                    && partNumbers.get(partNumbers.size() - 1) < indexRect.size();
        } else {
            throw new IOException("invalid classname: " + className);
        }
    }

    private boolean isGeometryClass(Class<?> clazz) {
        return clazz == org.locationtech.jts.geom.Point.class
                || clazz == org.locationtech.jts.geom.LineString.class
                || clazz == org.locationtech.jts.geom.MultiLineString.class
                || clazz == org.locationtech.jts.geom.Polygon.class
                || clazz == org.locationtech.jts.geom.MultiPolygon.class;
    }

    SimpleFeature buildFeature(GenericRecord record, Geometry geometry)
            throws IllegalArgumentException, NoSuchElementException {
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

    String getPartitionRoot(Configuration conf) throws IOException {
        String rootPath = fileDirectory.replace("\\", "/").trim();
        if (rootPath.endsWith("/")) {
            rootPath = rootPath.substring(0, rootPath.length() - 1);
        }

        while (currentIndex < partNumbers.size()) {
            NumberFormat numfmt = NumberFormat.getInstance(Locale.US);
            numfmt.setMinimumIntegerDigits(6);
            numfmt.setGroupingUsed(false);
            String partitionPath = rootPath + "/part-" + numfmt.format(currentIndex);

            Path fPath = new Path(partitionPath);
            if (!fPath.getFileSystem(conf).exists(fPath)) {
                currentIndex++;
                continue;
            }

            return partitionPath;
        }

        return "";
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

    /**
     * 是否已经读取
     *
     * @param index 索引
     * @param newPartNumbers 对象包含的分区编号
     * @return true or false
     */
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

    /**
     * 是否是最新版本
     *
     * @return true or false
     */
    boolean isLastedVersion(String id, long time) {
        if (deleteFeatures.size() > 0 && deleteFeatures.contains(id)) {
            return false;
        }

        if (modifiedFeatures.size() > 0) {
            long lastTime = modifiedFeatures.get(id);
            return lastTime <= time;
        }

        return true;
    }

    @Override
    public SimpleFeatureType getFeatureType() {
        return schema;
    }
}
