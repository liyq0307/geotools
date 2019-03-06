package org.geotools.data.supermapindexfile;

import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.Query;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.map.FeatureLayer;
import org.geotools.map.MapContent;
import org.geotools.styling.SLD;
import org.geotools.styling.Style;
import org.geotools.swing.JMapFrame;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;

import java.awt.*;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by liyq on 2019/3/5.
 */
public class SuperMapIndexFileDisPlayTest {
    public static void main(String[] args) throws IOException {
        Map<String, Serializable> params = new HashMap<>();
        String path = "file://" + SuperMapIndexFileDisPlayTest.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        params.put(SuperMapIndexFileDataStoreFactory.InputFile.key, path + "/data/world");

        String typeName = "world";
        DataStore dataStore = DataStoreFinder.getDataStore(params);
        SimpleFeatureSource fs = dataStore.getFeatureSource(typeName);

        ReferencedEnvelope bbox = new ReferencedEnvelope(-29.7200209437253, 84.9462682397991, -26.2363814116757, 66.7224810889989, null);
        FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();
        Filter filter = ff.bbox(ff.property("geom"), bbox);
        Query query = new Query(typeName, filter);
        SimpleFeatureCollection collection = fs.getFeatures(query);

        MapContent map = new MapContent();
        map.setTitle("SuperMapIndexFileDisPlayTest");

        Style style = SLD.createSimpleStyle(fs.getSchema(), Color.red);
        FeatureLayer layer = new FeatureLayer(collection, style);
        map.addLayer(layer);

        JMapFrame show = new JMapFrame(map);
        // list layers and set them as visible + selected
        show.enableLayerTable(true);
        // zoom in, zoom out, pan, show all
        show.enableToolBar(true);
        // location of cursor and bounds of current
        show.enableStatusBar(true);
        show.initComponents();
        show.setSize(800, 600);
        // display
        show.setVisible(true);
    }
}
