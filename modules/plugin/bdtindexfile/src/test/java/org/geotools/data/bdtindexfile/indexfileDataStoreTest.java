package org.geotools.data.bdtindexfile;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.geotools.data.*;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;

public class indexfileDataStoreTest {

    public static void main(String[] args) throws IOException {

        Map<String, Serializable> params = new HashMap<String, Serializable>();
        //params.put("BDTIndexFilePath", "/F:/indexOSMPTest");
        params.put("BDTIndexFilePath", "hdfs://localhost:9000/indexOSMPTest1");

        long lCount = 0;
        long startTime = System.currentTimeMillis(); // 开始时间
        DataStore dataStore1 = DataStoreFinder.getDataStore(params);
        String[] typeNames1 = dataStore1.getTypeNames();
        Query query1 = new Query(typeNames1[0]);
        FeatureReader<SimpleFeatureType, SimpleFeature> reader1 =
                dataStore1.getFeatureReader(query1, Transaction.AUTO_COMMIT);
        while (reader1.hasNext()) {
            reader1.next();
            lCount += 1;
        }
        reader1.close();
        long endTime = System.currentTimeMillis(); // 结束时间
        System.out.println("全部读取时间：" + (endTime - startTime) + "ms");
        System.out.println("全部读取对象个数：" + lCount);


        lCount = 0;
        ReferencedEnvelope bbox = new ReferencedEnvelope(35, 44.55, 45, 55, null);
        FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();
        Filter filter = ff.bbox(ff.property("*geom"), bbox);
        startTime = System.currentTimeMillis(); // 开始时间
        DataStore dataStore2 = DataStoreFinder.getDataStore(params);
        String[] typeNames2 = dataStore2.getTypeNames();
        Query query2 = new Query(typeNames2[0], filter);
        FeatureReader<SimpleFeatureType, SimpleFeature> reader2 =
                dataStore2.getFeatureReader(query2, Transaction.AUTO_COMMIT);
        while (reader2.hasNext()) {
            reader2.next();
            lCount += 1;
        }
        reader2.close();
        endTime = System.currentTimeMillis(); // 结束时间
        System.out.println("范围查询读取时间：" + (endTime - startTime) + "ms");
        System.out.println("范围查询读取对象个数：" + lCount);
    }
}
