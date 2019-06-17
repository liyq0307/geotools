package org.geotools.data.supermapdsf;

import static org.geotools.data.supermapdsf.SuperMapDSFUtils.PARQUET;

import com.alibaba.fastjson.JSONReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.locationtech.jts.geom.*;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.geometry.BoundingBox;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

/** Created by liyq on 2019/3/4. */
public class SuperMapDSFFeatureSource extends ContentFeatureSource {
    private SuperMapDSFDataStore dataStore;

    /**
     * Creates the new feature source from a query.
     *
     * <p>The <tt>query</tt> is taken into account for any operations done against the feature
     * source. For example, when getReader(Query) is called the query specified is "joined" to the
     * query specified in the constructor. The <tt>query</tt> parameter may be <code>null</code> to
     * specify that the feature source represents the entire set of features.
     *
     * @param entry ContentEntry
     * @param query Query
     * @param dataStore SuperMapDSFDataStore
     */
    SuperMapDSFFeatureSource(ContentEntry entry, Query query, SuperMapDSFDataStore dataStore) {
        super(entry, query);
        this.dataStore = dataStore;
    }

    private ReferencedEnvelope parserFromJson(String json, CoordinateReferenceSystem crs)
            throws IOException {
        StringReader reader = new StringReader(json);
        JSONReader jsonReader = new JSONReader(reader);
        jsonReader.startObject();

        ReferencedEnvelope envelope = null;
        while (jsonReader.hasNext()) {
            String str = jsonReader.readString();
            if (str.compareToIgnoreCase("grid") == 0) {
                jsonReader.startObject();
                Map<String, Object> paris = SuperMapDSFUtils.parseReader(jsonReader, crs);
                envelope = (ReferencedEnvelope) paris.get("bounds");
                jsonReader.endObject();
            } else if (str.compareToIgnoreCase("indexesMap") == 0) {
                jsonReader.readObject(Integer[].class);
            } else if (str.compareToIgnoreCase("indexesRect") == 0) {
                jsonReader.startArray();
                while (jsonReader.hasNext()) {
                    jsonReader.startArray();
                    for (int i = 0; i < 4; i++) {
                        jsonReader.readObject(Double.class);
                    }
                    jsonReader.endArray();
                }
                jsonReader.endArray();
            } else {
                throw new IOException("invalid key: " + str);
            }
        }

        jsonReader.endObject();
        jsonReader.close();
        reader.close();

        return envelope;
    }

    @Override
    protected ReferencedEnvelope getBoundsInternal(Query query) throws IOException {
        if (null == dataStore) {
            return null;
        }

        ReferencedEnvelope envelope = null;
        Filter filter = query.getFilter();
        if (null == filter || filter == Filter.INCLUDE) {
            StringReader inputStream = new StringReader(dataStore.getIndexJson());
            JSONReader jsonReader = new JSONReader(inputStream);

            jsonReader.startObject();
            String key = "";
            for (int i = 0; i < 3; i++) {
                key = jsonReader.readString();
            }

            if (key.compareToIgnoreCase("context") == 0) {
                envelope =
                        parserFromJson(
                                jsonReader.readString(),
                                SuperMapDSFUtils.getCRS(dataStore.getSchema()));
            }

            jsonReader.endObject();
            jsonReader.close();
            inputStream.close();
        } else {
            FeatureReader<SimpleFeatureType, SimpleFeature> reader = getReaderInternal(query);
            while (reader.hasNext()) {
                SimpleFeature feature = reader.next();
                if (null != feature) {
                    BoundingBox bounds = feature.getDefaultGeometryProperty().getBounds();
                    if (null == envelope) {
                        envelope = new ReferencedEnvelope(bounds);
                    } else {
                        envelope.expandToInclude(new ReferencedEnvelope(bounds));
                    }
                }
            }
        }

        return envelope;
    }

    @Override
    protected int getCountInternal(Query query) throws IOException {
        if (null == dataStore) {
            return 0;
        }

        int count = 0;

        if (null != query.getFilter() && query.getFilter() == Filter.INCLUDE) {
            String metaFile = dataStore.getFileDirectory() + "/.__meta";
            Path fPath = new Path(metaFile);
            FileSystem fs = fPath.getFileSystem(new Configuration());
            if (!fs.exists(fPath)) {
                return 0;
            }

            FSDataInputStream inputStream = fs.open(fPath);
            InputStreamReader streamReader = new InputStreamReader(inputStream, "UTF-8");
            JSONReader jsonReader = new JSONReader(streamReader);
            Map<String, String> values = new HashMap<>();
            jsonReader.startObject();
            while (jsonReader.hasNext()) {
                String key = jsonReader.readString();
                String value = jsonReader.readString();
                values.put(key, value);
            }
            jsonReader.endObject();

            jsonReader.close();
            streamReader.close();
            inputStream.close();

            count = Integer.parseInt(values.getOrDefault("RecordCount", "0"));
        } else {
            FeatureReader<SimpleFeatureType, SimpleFeature> reader = getReaderInternal(query);
            while (reader.hasNext() && reader.next() != null) {
                count++;
            }
        }

        return count;
    }

    @Override
    protected FeatureReader<SimpleFeatureType, SimpleFeature> getReaderInternal(Query query)
            throws IOException {
        if (null == dataStore) {
            return null;
        }

        StringReader inputStream = new StringReader(dataStore.getIndexJson());
        JSONReader jsonReader = new JSONReader(inputStream);

        SuperMapDSFFeatureReader indexReader;
        SimpleFeatureType sft = getSchema();

        jsonReader.startObject();
        String key = jsonReader.readString();
        if (key.compareToIgnoreCase("classname") == 0) {
            String className = jsonReader.readString();
            if (dataStore.getStorageFormat().compareToIgnoreCase(PARQUET) == 0) {
                indexReader = new ParquetFeatureReader(sft, query);
                indexReader.setClassName(className);
                indexReader.setCompress(dataStore.getCompress());
                indexReader.setFileDirectory(dataStore.getFileDirectory());
            } else {
                throw new IOException("invalid StorageFormat: " + dataStore.getStorageFormat());
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

        if (indexReader.initPartFile()) {
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
        if (!typeName.equals(dataStore.getSftName())) {
            return null;
        }

        SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
        String[] featureSpec = dataStore.getSftSpec().split(";");
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
                    builder.add(
                            fieldInfos[0].substring(1, fieldInfos[0].length()),
                            Point.class,
                            dataStore.getCrs());
                    break;
                case "LineString":
                    builder.add(
                            fieldInfos[0].substring(1, fieldInfos[0].length()),
                            LineString.class,
                            dataStore.getCrs());
                    break;
                case "MultiLineString":
                    builder.add(
                            fieldInfos[0].substring(1, fieldInfos[0].length()),
                            MultiLineString.class,
                            dataStore.getCrs());
                    break;
                case "Polygon":
                    builder.add(
                            fieldInfos[0].substring(1, fieldInfos[0].length()),
                            Polygon.class,
                            dataStore.getCrs());
                    break;
                case "MultiPolygon":
                    builder.add(
                            fieldInfos[0].substring(1, fieldInfos[0].length()),
                            MultiPolygon.class,
                            dataStore.getCrs());
                    break;
                default:
                    throw new IOException("Unknown field type");
            }
        }

        builder.setName(typeName);
        builder.setCRS(dataStore.getCrs());

        SimpleFeatureType sft = builder.buildFeatureType();
        if (featureSpec.length > 1) {
            String[] userDataSpecs = featureSpec[1].split(",");
            for (String userDatSpec : userDataSpecs) {
                String[] userData = userDatSpec.split("=");
                if (userData.length == 2) {
                    sft.getUserData().put(userData[0], userData[1]);
                }
            }
        }

        return sft;
    }
}
