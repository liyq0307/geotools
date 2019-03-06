package org.geotools.data.supermapindexfile;

import com.alibaba.fastjson.JSONReader;
import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.io.IOException;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Map;

import static org.geotools.data.supermapindexfile.SuperMapIndexFileUtils.*;

/**
 * Created by liyq on 2019/3/4.
 */
public class SuperMapIndexFileFeatureSource extends ContentFeatureSource {
    private SuperMapIndexFileDataStore dataStore;

    /**
     * Creates the new feature source from a query.
     *
     * <p>The <tt>query</tt> is taken into account for any operations done against the feature
     * source. For example, when getReader(Query) is called the query specified is "joined" to the
     * query specified in the constructor. The <tt>query</tt> parameter may be <code>null</code> to
     * specify that the feature source represents the entire set of features.
     *
     * @param entry
     * @param query
     */
    public SuperMapIndexFileFeatureSource(ContentEntry entry, Query query, SuperMapIndexFileDataStore dataStore) {
        super(entry, query);
        this.dataStore = dataStore;
    }

    private ReferencedEnvelope parserFromJson(String json, CoordinateReferenceSystem crs) throws IOException {
        StringReader reader = new StringReader(json);
        JSONReader jsonReader = new JSONReader(reader);
        jsonReader.startObject();

        ReferencedEnvelope envelope = null;
        while (jsonReader.hasNext()) {
            if (jsonReader.readString().compareToIgnoreCase("grid") == 0) {
                jsonReader.startObject();
                Map<String, Object> paris = SuperMapIndexFileUtils.parseReader(jsonReader, crs);
                envelope = (ReferencedEnvelope)paris.get("bounds");
                jsonReader.endObject();
            }
        }

        reader.close();

        return envelope;
    }

    @Override
    protected ReferencedEnvelope getBoundsInternal(Query query) throws IOException {
        if (null == dataStore) {
            return null;
        }

        Filter filter = query.getFilter();
        if (null == filter || filter == Filter.INCLUDE) {
            StringReader inputStream = new StringReader(dataStore.indexJson);
            JSONReader jsonReader = new JSONReader(inputStream);

            jsonReader.startObject();
            String key = "";
            for (int i = 0; i < 3; i++) {
                key = jsonReader.readString();
            }

            ReferencedEnvelope envelope = null;
            if (key.compareToIgnoreCase("context") == 0) {
                envelope = parserFromJson(jsonReader.readString(), SuperMapIndexFileUtils.getCRS(dataStore.getSchema()));
            }

            jsonReader.endObject();
            jsonReader.close();
            inputStream.close();

            return envelope;
        }

        return null;
    }

    @Override
    protected int getCountInternal(Query query) throws IOException {
        return 0;
    }

    @Override
    protected FeatureReader<SimpleFeatureType, SimpleFeature> getReaderInternal(Query query) throws IOException {
        if (null == dataStore) {
            return null;
        }

        StringReader inputStream = new StringReader(dataStore.indexJson);
        JSONReader jsonReader = new JSONReader(inputStream);

        SuperMapIndexFileFeatureReader indexReader;
        SimpleFeatureType sft = getSchema();

        jsonReader.startObject();
        String key = jsonReader.readString();
        if (key.compareToIgnoreCase("classname") == 0) {
            String name = jsonReader.readString();
            if (name.compareToIgnoreCase(PARTITIONQUADTREE) == 0) {
                if (dataStore.storageFormat.compareToIgnoreCase(AVRO) == 0) {
                    indexReader = new QuadAvroFeatureReader(sft, dataStore.fileDirectory);
                } else if (dataStore.storageFormat.compareToIgnoreCase(PARQUETSPATIAL) == 0) {
                    indexReader = new QuadParquetFeatureReader(sft, dataStore.fileDirectory);
                } else {
                    throw new IOException("invalid StorageFormat: " + dataStore.storageFormat);
                }
            } else if (name.compareToIgnoreCase(PARTITIONGRID) == 0) {
                if (dataStore.storageFormat.compareToIgnoreCase(AVRO) == 0) {
                    indexReader = new GridAvroFeatureReader(sft, dataStore.fileDirectory);
                } else if (dataStore.storageFormat.compareToIgnoreCase(PARQUETSPATIAL) == 0) {
                    indexReader = new GridParquetFeatureReader(sft, dataStore.fileDirectory);
                } else {
                    throw new IOException("invalid StorageFormat: " + dataStore.storageFormat);
                }
            } else {
                throw new IOException("invalid classname: " + name);
            }
        } else {
            throw new IOException("invalid json tag: " + key);
        }

        key = jsonReader.readString();
        if (key.compareToIgnoreCase("context") == 0) {
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
    protected SimpleFeatureType buildFeatureType() throws IOException {
        if (null == dataStore) {
            return null;
        }

        String typeName = getEntry().getTypeName();
        if (!typeName.equals(dataStore.sftName)) {
            return null;
        }

        SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
        String[] featureSpec = dataStore.sftSpec.split(";");
        if (featureSpec.length < 1) {
            throw new IOException("error sftSpec");
        }

        String[] fieldNameTypes = featureSpec[0].split(",");
        for (String fieldNameType : fieldNameTypes) {
            String[] fieldInfos = fieldNameType.split(":");
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
                    builder.add(fieldInfos[0].substring(1, fieldInfos[0].length()), Point.class);
                    break;
                case "MultiLineString":
                    builder.add(fieldInfos[0].substring(1, fieldInfos[0].length()), MultiLineString.class);
                    break;
                case "MultiPolygon":
                    builder.add(fieldInfos[0].substring(1, fieldInfos[0].length()), MultiPolygon.class);
                    break;
                default:
                    throw new IOException("Unknown field type");
            }
        }

        builder.setName(typeName);

        SimpleFeatureType sft = builder.buildFeatureType();
        if (featureSpec.length > 1) {
            String[] userDataSpecs = featureSpec[1].split(",");
            for (String userDatSpec: userDataSpecs) {
                String[] userData = userDatSpec.split("=");
                if (userData.length == 2) {
                    sft.getUserData().put(userData[0], userData[1]);
                }
            }
        }

        return sft;
    }
}
