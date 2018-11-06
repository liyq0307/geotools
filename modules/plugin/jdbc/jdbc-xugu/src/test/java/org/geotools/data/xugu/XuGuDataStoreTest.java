package org.geotools.data.xugu;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.Query;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.geotools.geometry.jts.JTS;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by liyq on 2018/11/1.
 */
public class XuGuDataStoreTest {
    public static void main(String args[]) {
        Map<String, Object> params = new HashMap<>();
        params.put("dbtype", "xugu");
        params.put("host", "127.0.0.1");
        params.put("port", 5138);
        params.put("schema", "SYSDBA");
        params.put("database", "test");
        params.put("user", "SYSDBA");
        params.put("passwd", "SYSDBA");
        params.put("parallel", 8);

        try {
            DataStore dataStore = DataStoreFinder.getDataStore(params);

            /* 测试获取SimpleFeatureType和投影 */
            SimpleFeatureType sft = dataStore.getSchema("CHINALATESTRGRID");
            CoordinateReferenceSystem crs = sft.getCoordinateReferenceSystem();
            println("投影：" + crs.toWKT());

            SimpleFeatureSource sf = dataStore.getFeatureSource("CHINALATESTRGRID");

            /* bounds查询*/
            Envelope envelope = new Envelope(104.1599943, 123.15694, 13.9487362, 34.3134506);
            ReferencedEnvelope bBox = new ReferencedEnvelope(envelope, null);
            Geometry geometry = JTS.toGeometry(bBox);
            String sql = "INTERSECTS(" + geometry.toText() + ", SMGEOMETRY)";
            /*字段查询*/
            //String sql = "OSM_ID > 36677744 AND OSM_ID < 36775183";
            /*BBOX查询*/
            //String sql= "BBOX(SMGEOMETRY,104.1599943,123.15694,13.9487362,34.3134506)"

            Filter filter = ECQL.toFilter(sql);
            Query query = new Query("CHINALATESTRGRID", filter);
            ReferencedEnvelope bb = sf.getBounds(query);
            println("bounds范围：" + bb);

            SimpleFeatureIterator sfsIter = sf.getFeatures(query).features();
            int count = 0;
            while (sfsIter.hasNext()) {
                sfsIter.next();
                count++;
            }
            println("记录数：" + count);
        } catch (IOException | CQLException e) {
            e.printStackTrace();
        }
    }

    private static void println(Object s) {
        System.out.println(s);
    }
}
