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
import java.awt.*;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFactorySpi;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

public class SuperMapDSFDataStoreFactory implements DataStoreFactorySpi {

    static final Param InputFile = new Param("path", String.class, "Directory of DSF", true);

    @Override
    public DataStore createDataStore(Map<String, Serializable> params) throws IOException {
        String fileDirectory = (String) InputFile.lookUp(params);
        Configuration conf = new Configuration();
        if (fileDirectory.toLowerCase().startsWith("oss://")) {
            conf.set("fs.oss.impl", "org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem");
        }
        String filePath = getIndexFile(conf, fileDirectory);

        Path path = new Path(filePath);
        FileSystem fs = path.getFileSystem(conf);
        FSDataInputStream fileRead = fs.open(path);
        InputStreamReader inputStream = new InputStreamReader(fileRead, "UTF-8");

        String sft_name = "";
        String sft_spec = "";
        String indexJson = "";
        String storageFormat = "";
        CoordinateReferenceSystem crs = null;
        String compressName = "UnCompressed";
        String version = "";
        JSONReader jsonReader = new JSONReader(inputStream);
        jsonReader.startObject();
        while (jsonReader.hasNext()) {
            String key = jsonReader.readString().toUpperCase();
            switch (key) {
                case TYPENAME:
                    sft_name = jsonReader.readString();
                    break;
                case TYPESPEC:
                    sft_spec = jsonReader.readString();
                    break;
                case INDEXER:
                    indexJson = jsonReader.readString();
                    break;
                case FORMAT:
                    storageFormat = jsonReader.readString();
                    break;
                case SRS:
                    crs = SuperMapDSFUtils.getCRS(jsonReader.readString());
                    break;
                case COMPRESSION:
                    compressName = jsonReader.readString();
                    break;
                case VERSION:
                    version = jsonReader.readString();
                    break;
                default:
                    jsonReader.readObject();
                    break;
            }
        }

        jsonReader.endObject();
        jsonReader.close();
        inputStream.close();
        fileRead.close();

        SuperMapDSFDataStore store = new SuperMapDSFDataStore(fileDirectory);
        store.setSftName(sft_name);
        store.setSftSpec(sft_spec);
        store.setIndexJson(indexJson);
        store.setStorageFormat(storageFormat);
        store.setCRS(crs);
        store.setCompress(compressName);
        store.setVersion(version);

        return store;
    }

    @Override
    public String getDisplayName() {
        return "分布式空间文件引擎(DSF)";
    }

    @Override
    public String getDescription() {
        return "Takes a directory of DSF and exposes it as a data store";
    }

    @Override
    public Param[] getParametersInfo() {
        return new Param[] {InputFile};
    }

    @Override
    public boolean canProcess(Map<String, Serializable> params) {
        try {
            String filePath = (String) InputFile.lookUp(params);
            if (filePath == null || filePath.isEmpty()) {
                return false;
            }

            Configuration conf = new Configuration();
            Path path = new Path(filePath);
            FileSystem fs = path.getFileSystem(conf);
            if (!fs.exists(path)) {
                return false;
            }

            if (fs.isDirectory(path)) {
                return haveIndex(conf, filePath);
            }

            return false;
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    public boolean isAvailable() {
        return true;
    }

    @Override
    public DataStore createNewDataStore(Map<String, Serializable> params) throws IOException {
        return createDataStore(params);
    }

    @Override
    public Map<RenderingHints.Key, ?> getImplementationHints() {
        return Collections.EMPTY_MAP;
    }
}
