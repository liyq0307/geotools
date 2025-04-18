/*
 *    GeoTools - The Open Source Java GIS Toolkit
 *    http://geotools.org
 *
 *    (C) 2007-2016, Open Source Geospatial Foundation (OSGeo)
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
package org.geotools.data.shapefile;

import java.io.IOException;
import java.util.Set;
import org.geotools.api.data.FeatureReader;
import org.geotools.api.data.FeatureWriter;
import org.geotools.api.data.Query;
import org.geotools.api.data.QueryCapabilities;
import org.geotools.api.data.ResourceInfo;
import org.geotools.api.data.Transaction;
import org.geotools.api.feature.FeatureVisitor;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.feature.type.Name;
import org.geotools.api.filter.Filter;
import org.geotools.data.FilteringFeatureWriter;
import org.geotools.data.shapefile.files.ShpFiles;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureStore;
import org.geotools.data.store.ContentState;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.util.factory.Hints.Key;

/**
 * FeatureStore for the Shapefile store, based on the {@link ContentFeatureStore} framework
 *
 * @author Andrea Aime - GeoSolutions
 */
class ShapefileFeatureStore extends ContentFeatureStore {

    ShapefileFeatureSource delegate;

    @SuppressWarnings("unchecked")
    public ShapefileFeatureStore(ContentEntry entry, ShpFiles files) {
        super(entry, Query.ALL);
        this.delegate = new ShapefileFeatureSource(entry, files);
        this.hints = (Set<Key>) (Set<?>) delegate.getSupportedHints();
    }

    @Override
    protected FeatureWriter<SimpleFeatureType, SimpleFeature> getWriterInternal(Query query, int flags)
            throws IOException {
        if (flags == 0) {
            throw new IllegalArgumentException("no write flags set");
        }

        @SuppressWarnings("PMD.CloseResource") // managed as part of the writer
        ShapefileFeatureReader reader = (ShapefileFeatureReader) delegate.getReaderInternal(Query.ALL);
        ShapefileFeatureWriter writer;
        ShapefileDataStore ds = getDataStore();
        if (ds.indexManager.hasFidIndex(false) || ds.isFidIndexed() && ds.indexManager.hasFidIndex(true)) {
            writer = new IndexedShapefileFeatureWriter(ds.indexManager, reader, ds.getCharset(), ds.getTimeZone());
        } else {
            writer = new ShapefileFeatureWriter(delegate.shpFiles, reader, ds.getCharset(), ds.getTimeZone());
        }
        writer.setMaxShpSize(getDataStore().getMaxShpSize());
        writer.setMaxDbfSize(getDataStore().getMaxDbfSize());

        // if we only have to add move to the end.
        // TODO: just make the code transfer the bytes in bulk instead and start actual writing at
        // the end
        if ((flags | WRITER_ADD) == WRITER_ADD) {
            while (writer.hasNext()) {
                writer.next();
            }
        }

        // if we are filtering wrap the writer so that it returns only the selected features
        // but writes down the mall
        Filter filter = query.getFilter();
        if (filter != null && !Filter.INCLUDE.equals(filter)) {
            return new FilteringFeatureWriter(writer, filter);
        } else {
            return writer;
        }
    }

    // ----------------------------------------------------------------------------------------
    // METHODS DELEGATED TO OGRFeatureSource
    // ----------------------------------------------------------------------------------------

    @Override
    public ShapefileDataStore getDataStore() {
        return delegate.getDataStore();
    }

    @Override
    public Transaction getTransaction() {
        return delegate.getTransaction();
    }

    @Override
    public ResourceInfo getInfo() {
        return delegate.getInfo();
    }

    @Override
    public QueryCapabilities getQueryCapabilities() {
        return delegate.getQueryCapabilities();
    }

    @Override
    protected ReferencedEnvelope getBoundsInternal(Query query) throws IOException {
        return delegate.getBoundsInternal(query);
    }

    @Override
    protected int getCountInternal(Query query) throws IOException {
        return delegate.getCountInternal(query);
    }

    @Override
    protected FeatureReader<SimpleFeatureType, SimpleFeature> getReaderInternal(Query query) throws IOException {
        return delegate.getReaderInternal(query);
    }

    @Override
    protected SimpleFeatureType buildFeatureType() throws IOException {
        return delegate.buildFeatureType();
    }

    @Override
    public ContentEntry getEntry() {
        return delegate.getEntry();
    }

    @Override
    public Name getName() {
        return delegate.getName();
    }

    @Override
    public ContentState getState() {
        return delegate.getState();
    }

    @Override
    public void setTransaction(Transaction transaction) {
        super.setTransaction(transaction);

        if (delegate.getTransaction() != transaction) {
            delegate.setTransaction(transaction);
        }
    }

    @Override
    protected boolean canFilter(Query query) {
        return delegate.canFilter(query);
    }

    @Override
    protected boolean canRetype(Query query) {
        return delegate.canRetype(query);
    }

    @Override
    protected boolean handleVisitor(Query query, FeatureVisitor visitor) throws IOException {
        return delegate.handleVisitor(query, visitor);
    }
}
