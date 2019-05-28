package org.geotools.data.supermapdsf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.geotools.data.*;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.store.ContentDataStore;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.feature.NameImpl;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.Name;
import org.opengis.filter.Filter;

/** Created by liyq on 2019/3/4. */
public class SuperMapDSFDataStore extends ContentDataStore implements FileDataStore {
    String fileDirectory;

    String sftName;

    String sftSpec;

    String indexJson;

    String storageFormat;

    void setSftName(String sftName) {
        this.sftName = sftName;
    }

    void setSftSpec(String sftSpec) {
        this.sftSpec = sftSpec;
    }

    void setIndexJson(String indexJson) {
        this.indexJson = indexJson;
    }

    void setStorageFormat(String storageFormat) {
        this.storageFormat = storageFormat;
    }

    SuperMapDSFDataStore(String filePath) {
        this.fileDirectory = filePath;
    }

    @Override
    public SimpleFeatureType getSchema() throws IOException {
        return getSchema(sftName);
    }

    @Override
    public SimpleFeatureSource getFeatureSource() throws IOException {
        return getFeatureSource(sftName);
    }

    @Override
    public FeatureReader<SimpleFeatureType, SimpleFeature> getFeatureReader() throws IOException {
        return getFeatureReader(new Query(sftName), Transaction.AUTO_COMMIT);
    }

    @Override
    protected List<Name> createTypeNames() throws IOException {
        if (sftName.isEmpty()) {
            return null;
        }

        List<Name> names = new ArrayList<>();
        names.add(new NameImpl(sftName));

        return names;
    }

    @Override
    protected ContentFeatureSource createFeatureSource(ContentEntry entry) throws IOException {
        return new SuperMapDSFFeatureSource(entry, Query.ALL, this);
    }

    @Override
    public void updateSchema(SimpleFeatureType featureType) throws IOException {}

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
}
