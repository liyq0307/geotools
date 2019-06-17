package org.geotools.data.supermapdsf;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.geotools.data.*;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;

public class SuperMapDSFDataStoreTest {
    public static void main(String[] args) throws IOException {
        Map<String, Serializable> params = new HashMap<>();
        //        String path =
        //                "file://"
        //                        + SuperMapDSFDisPlayTest.class
        //                                .getProtectionDomain()
        //                                .getCodeSource()
        //                                .getLocation()
        //                                .getPath();
        //        params.put(SuperMapDSFDataStoreFactory.InputFile.key, path + "/data/world");
        String path = "F:\\Data\\DSF\\world_quad";
        params.put(SuperMapDSFDataStoreFactory.InputFile.key, path);
        long lCount = 0;
        long startTime = System.currentTimeMillis(); // 开始时间
        DataStore dataStore = DataStoreFinder.getDataStore(params);
        String typeName = dataStore.getTypeNames()[0];
        SimpleFeatureSource featureSource = dataStore.getFeatureSource(typeName);
        ReferencedEnvelope envelope = featureSource.getBounds();
        //        try {
        //            // Filter filter = ECQL.toFilter("ColorID > 10 and ColorID < 20 or ColorID =
        // 15");
        //            Filter filter = ECQL.toFilter("IN(10, 20, 30)");
        //            FilterToDSF filterToDSF = new FilterToDSF(dataStore.getSchema(typeName), 0.0);
        //            FilterPredicate filterPredicate = (FilterPredicate) filter.accept(filterToDSF,
        // null);
        //            FilterCompat.Filter filter1 = FilterCompat.get(filterPredicate);
        //            System.out.println(filter1.toString());
        //        } catch (CQLException e) {
        //            e.printStackTrace();
        //        }
        System.out.println(envelope);
        SimpleFeatureIterator iterator = featureSource.getFeatures().features();
        while (iterator.hasNext()) {
            SimpleFeature feature = iterator.next();
            if (null != feature) {
                lCount++;
            } else {
                System.out.println("why?");
            }
        }

        long endTime = System.currentTimeMillis(); // 结束时间
        System.out.println("全部读取时间：" + (endTime - startTime) + "ms");
        System.out.println("全部读取对象个数：" + lCount);

        lCount = 0;
        ReferencedEnvelope bbox =
                new ReferencedEnvelope(
                        -29.7200209437253,
                        84.9462682397991,
                        -26.2363814116757,
                        66.7224810889989,
                        null);
        FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();
        // Filter filter = ff.bbox(ff.property("geom"), bbox);
        Filter filter = null;
        try {
            filter = ff.and(ECQL.toFilter("IN(10, 20, 30)"), ff.bbox(ff.property("geom"), bbox));
        } catch (CQLException e) {
            e.printStackTrace();
        }
        startTime = System.currentTimeMillis(); // 开始时间
        String[] typeNames2 = dataStore.getTypeNames();
        Query query2 = new Query(typeNames2[0], filter);
        FeatureReader<SimpleFeatureType, SimpleFeature> reader2 =
                dataStore.getFeatureReader(query2, Transaction.AUTO_COMMIT);
        while (reader2.hasNext()) {
            if (null != reader2.next()) {
                lCount += 1;
            }
        }
        reader2.close();
        endTime = System.currentTimeMillis(); // 结束时间
        System.out.println("范围查询读取时间：" + (endTime - startTime) + "ms");
        System.out.println("范围查询读取对象个数：" + lCount);

        dataStore.dispose();
    }
}
