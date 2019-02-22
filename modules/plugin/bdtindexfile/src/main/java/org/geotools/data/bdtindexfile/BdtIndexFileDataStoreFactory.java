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
import java.awt.*;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.URI;
import java.util.Collections;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFactorySpi;

public class BdtIndexFileDataStoreFactory implements DataStoreFactorySpi {

    public static final Param BDTIFP =
            new Param("BDTIndexFilePath", String.class, "Directory containing index files", true);

    @Override
    public DataStore createDataStore(Map<String, Serializable> params) throws IOException {
        String fileDirectory = (String) BDTIFP.lookUp(params);
        String filePath = fileDirectory + "/INDEXFILE.index";

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(filePath), conf);
        FSDataInputStream fileRead = fs.open(new Path(filePath));
        InputStreamReader inputStream = new InputStreamReader(fileRead, "UTF-8");

        String sft_name = "";
        String sft_spec = "";
        String indexJson = "";
        String storageFormat = "";
        JSONReader jsonReader = new JSONReader(inputStream);
        jsonReader.startObject();
        while (jsonReader.hasNext()) {
            String key = jsonReader.readString();
            switch (key.toUpperCase()) {
                case "SIMPLEFEATURETYPENAME":
                    sft_name = jsonReader.readString();
                    break;
                case "SIMPLEFEATURETYPESPEC":
                    sft_spec = jsonReader.readString();
                    break;
                case "INDEXER":
                    indexJson = jsonReader.readString();
                    break;
                case "STORAGEFORMAT":
                    storageFormat = jsonReader.readString();
                    break;
                default:
                    break;
            }
        }
        jsonReader.endObject();
        jsonReader.close();
        inputStream.close();
        fileRead.close();

        BdtIndexFileDataStore store = new BdtIndexFileDataStore(fileDirectory);
        store.setSftName(sft_name);
        store.setSftSpec(sft_spec);
        store.setIndexJson(indexJson);
        store.setStorageFormat(storageFormat);
        return store;
    }

    @Override
    public String getDisplayName() {
        return "Directory of index files (BDTIndexFiles)";
    }

    @Override
    public String getDescription() {
        return "Takes a directory of BDTIndexFiles and exposes it as a data store";
    }

    @Override
    public Param[] getParametersInfo() {
        return new Param[] {BDTIFP};
    }

    @Override
    public boolean canProcess(Map<String, Serializable> params) {
        try {
            String filePath = (String) BDTIFP.lookUp(params);
            if (filePath == null || filePath.isEmpty()) {
                return false;
            }
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(URI.create(filePath), conf);
            Path path = new Path(filePath);
            if (!fs.exists(path)) {
                return false;
            }
            if (fs.isDirectory(path)) {
                filePath = filePath + "/INDEXFILE.index";
                if (fs.exists(new Path(filePath))) {
                    return true;
                }
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
