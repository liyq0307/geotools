package org.geotools.data.supermapindexfile;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.geotools.data.*;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;

public class SuperMapIndexFileDataStoreTest {
    public static void main(String[] args) throws IOException {
        Map<String, Serializable> params = new HashMap<>();
        //params.put("BDTIndexFilePath", "/F:/indexOSMPTest");
        //params.put("BDTIndexFilePath", "hdfs://localhost:9000/indexOSMPTest1");
        String path = "file://" + SuperMapIndexFileDisPlayTest.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        params.put(SuperMapIndexFileDataStoreFactory.InputFile.key, path + "/data/world");

        long lCount = 0;
        long startTime = System.currentTimeMillis(); // 开始时间
        DataStore dataStore = DataStoreFinder.getDataStore(params);
        SimpleFeatureSource featureSource = dataStore.getFeatureSource("world");
        ReferencedEnvelope envelope = featureSource.getBounds();
        System.out.println(envelope);
        SimpleFeatureIterator iterator = featureSource.getFeatures().features();
        while (iterator.hasNext()) {
            iterator.next();
            lCount++;
        }

        long endTime = System.currentTimeMillis(); // 结束时间
        System.out.println("全部读取时间：" + (endTime - startTime) + "ms");
        System.out.println("全部读取对象个数：" + lCount);

        lCount = 0;
        ReferencedEnvelope bbox = new ReferencedEnvelope(-29.7200209437253, 84.9462682397991, -26.2363814116757, 66.7224810889989, null);
        FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();
        Filter filter = ff.bbox(ff.property("geom"), bbox);
        startTime = System.currentTimeMillis(); // 开始时间
        String[] typeNames2 = dataStore.getTypeNames();
        Query query2 = new Query(typeNames2[0], filter);
        FeatureReader<SimpleFeatureType, SimpleFeature> reader2 =
                dataStore.getFeatureReader(query2, Transaction.AUTO_COMMIT);
        while (reader2.hasNext()) {
            reader2.next();
            lCount += 1;
        }
        reader2.close();
        endTime = System.currentTimeMillis(); // 结束时间
        System.out.println("范围查询读取时间：" + (endTime - startTime) + "ms");
        System.out.println("范围查询读取对象个数：" + lCount);

        dataStore.dispose();
    }
}
