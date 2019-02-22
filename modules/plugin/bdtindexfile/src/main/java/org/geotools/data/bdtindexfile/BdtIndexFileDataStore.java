/*
 *    GeoTools - The Open Source Java GIS Toolkit
 *    http://geotools.org
 *
 *    (C) 2002-2015, Open Source Geospatial Foundation (OSGeo)
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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.geotools.data.*;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.feature.NameImpl;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.Name;
import org.opengis.filter.Filter;

public class BdtIndexFileDataStore implements FileDataStore {

    String fileDirectory;

    String sftName;

    String sftSpec;

    String indexJson;

    String storageFormat;

    public BdtIndexFileDataStore(String filePath) {
        fileDirectory = filePath;
    }

    public void setSftName(String sftName) {
        this.sftName = sftName;
    }

    public void setSftSpec(String sftSpec) {
        this.sftSpec = sftSpec;
    }

    public void setIndexJson(String indexJson) {
        this.indexJson = indexJson;
    }

    public void setStorageFormat(String storageFormat) {
        this.storageFormat = storageFormat;
    }

    @Override
    public SimpleFeatureType getSchema() throws IOException {
        return getSchema(this.sftName);
    }

    @Override
    public void updateSchema(SimpleFeatureType featureType) throws IOException {}

    @Override
    public SimpleFeatureSource getFeatureSource() throws IOException {
        return getFeatureSource(this.sftName);
    }

    @Override
    public FeatureReader<SimpleFeatureType, SimpleFeature> getFeatureReader() throws IOException {
        return getFeatureReader(new Query(this.sftName), Transaction.AUTO_COMMIT);
    }

    @Override
    public FeatureWriter<SimpleFeatureType, SimpleFeature> getFeatureWriter(
            Filter filter, Transaction transaction) throws IOException {
        return null;
    }

    @Override
    public FeatureWriter<SimpleFeatureType, SimpleFeature> getFeatureWriter(Transaction transaction)
            throws IOException {
        return null;
    }

    @Override
    public FeatureWriter<SimpleFeatureType, SimpleFeature> getFeatureWriterAppend(
            Transaction transaction) throws IOException {
        return null;
    }

    @Override
    public void updateSchema(String typeName, SimpleFeatureType featureType) throws IOException {}

    @Override
    public void removeSchema(String typeName) throws IOException {}

    @Override
    public String[] getTypeNames() throws IOException {
        if (sftName.isEmpty()) {
            return new String[0];
        }
        return new String[] {sftName};
    }

    @Override
    public SimpleFeatureType getSchema(String typeName) throws IOException {
        if (!typeName.equals(sftName)) {
            return null;
        }

        SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
        String[] fieldNameTypes = sftSpec.split(",");
        for (int i = 0; i < fieldNameTypes.length; i++) {
            String[] fieldInfos = fieldNameTypes[i].split(":");
            if (fieldInfos.length != 2) {
                throw new IOException("Error parsing field");
            }
            switch (fieldInfos[1]) {
                case "Integer":
                    builder.add(fieldInfos[0], Integer.class);
                    break;
                case "Long":
                    builder.add(fieldInfos[0], Long.class);
                    break;
                case "Double":
                    builder.add(fieldInfos[0], Double.class);
                    break;
                case "Float":
                    builder.add(fieldInfos[0], Float.class);
                    break;
                case "String":
                    builder.add(fieldInfos[0], String.class);
                    break;
                case "Boolean":
                    builder.add(fieldInfos[0], Boolean.class);
                    break;
                case "Timestamp":
                    builder.add(fieldInfos[0], Date.class);
                    break;
                case "Bytes":
                    builder.add(fieldInfos[0], ByteBuffer.class);
                    break;
                case "Point":
                    builder.add(fieldInfos[0], Point.class);
                    break;
                case "MultiLineString":
                    builder.add(fieldInfos[0], MultiLineString.class);
                    break;
                case "MultiPolygon":
                    builder.add(fieldInfos[0], MultiPolygon.class);
                    break;
                default:
                    throw new IOException("Unknown field type");
            }
        }
        builder.setName(typeName);
        return builder.buildFeatureType();
    }

    @Override
    public SimpleFeatureSource getFeatureSource(String typeName) throws IOException {
        return null;
    }

    @Override
    public ServiceInfo getInfo() {
        return null;
    }

    @Override
    public void createSchema(SimpleFeatureType featureType) throws IOException {}

    @Override
    public void updateSchema(Name typeName, SimpleFeatureType featureType) throws IOException {}

    @Override
    public void removeSchema(Name typeName) throws IOException {}

    @Override
    public List<Name> getNames() throws IOException {
        if (sftName.isEmpty()) {
            return null;
        }
        List<Name> names = new ArrayList<Name>(1);
        names.add(new NameImpl(sftName));
        return names;
    }

    @Override
    public SimpleFeatureType getSchema(Name name) throws IOException {
        return getSchema(name.getLocalPart());
    }

    @Override
    public SimpleFeatureSource getFeatureSource(Name typeName) throws IOException {
        return getFeatureSource(typeName.getLocalPart());
    }

    @Override
    public void dispose() {}

    @Override
    public FeatureReader<SimpleFeatureType, SimpleFeature> getFeatureReader(
            Query query, Transaction transaction) throws IOException {
        if (query.getTypeName() == null) {
            throw new IOException("Query does not specify type.");
        }
        if (!query.getTypeName().equals(this.sftName)) {
            return null;
        }

        StringReader inputStream = new StringReader(indexJson);
        JSONReader jsonReader = new JSONReader(inputStream);

        BdtIndexFileFeatureReader indexReader = null;
        SimpleFeatureType sft = getSchema(this.sftName);

        jsonReader.startObject();
        String key = jsonReader.readString();
        if (key.toLowerCase().compareTo("classname") == 0) {
            String name = jsonReader.readString();
            if (name.toLowerCase().compareTo("com.supermap.bdt.rdd.index.impl.partitionquadtree")
                    == 0) {
                if (storageFormat.toLowerCase().compareTo("avro") == 0) {
                    indexReader = new QuadAvroFeatureReader(sft, fileDirectory);
                } else if (storageFormat.toLowerCase().compareTo("parquetspatial") == 0) {
                    indexReader = new QuadParquetFeatureReader(sft, fileDirectory);
                } else {
                    throw new IOException("invalid StorageFormat: " + storageFormat);
                }
            } else if (name.toLowerCase().compareTo("com.supermap.bdt.rdd.index.impl.partitiongrid")
                    == 0) {
                if (storageFormat.toLowerCase().compareTo("avro") == 0) {
                    indexReader = new GridAvroFeatureReader(sft, fileDirectory);
                } else if (storageFormat.toLowerCase().compareTo("parquetspatial") == 0) {
                    indexReader = new GridParquetFeatureReader(sft, fileDirectory);
                } else {
                    throw new IOException("invalid StorageFormat: " + storageFormat);
                }
            } else {
                throw new IOException("invalid classname: " + name);
            }
        } else {
            throw new IOException("invalid json tag: " + key);
        }

        key = jsonReader.readString();
        if (key.toLowerCase().compareTo("context") == 0) {
            indexReader.fromJson(jsonReader.readString());
        } else {
            throw new IOException("invalid json tag: " + key);
        }

        jsonReader.endObject();
        jsonReader.close();
        inputStream.close();

        if (indexReader.initPartFile(query.getFilter())) {
            return indexReader;
        }
        return null;
    }

    @Override
    public FeatureWriter<SimpleFeatureType, SimpleFeature> getFeatureWriter(
            String typeName, Filter filter, Transaction transaction) throws IOException {
        return null;
    }

    @Override
    public FeatureWriter<SimpleFeatureType, SimpleFeature> getFeatureWriter(
            String typeName, Transaction transaction) throws IOException {
        return null;
    }

    @Override
    public FeatureWriter<SimpleFeatureType, SimpleFeature> getFeatureWriterAppend(
            String typeName, Transaction transaction) throws IOException {
        return null;
    }

    @Override
    public LockingManager getLockingManager() {
        return null;
    }
}
