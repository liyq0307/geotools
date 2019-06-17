package org.geotools.data.supermapdsf;

import com.alibaba.fastjson.JSONReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.geometry.BoundingBox;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

/** Created by liyq on 2019/3/5. */
class SuperMapDSFUtils {
    static String getIndexFile(Configuration conf, String filePath) throws IOException {
        if (filePath == null || filePath.isEmpty()) {
            throw new IllegalArgumentException("filePath is empty");
        }

        if (filePath.endsWith("INDEXFILE.index")) {
            return filePath;
        } else {
            Path path = new Path(filePath);
            FileSystem fs = path.getFileSystem(conf);

            if (!fs.exists(path)) {
                throw new IllegalArgumentException("filePath " + filePath + " is not existed");
            }

            if (fs.isDirectory(path)) {
                return filePath + "/INDEXFILE.index";
            } else {
                throw new IllegalArgumentException("filePath " + filePath + " must be directory");
            }
        }
    }

    static Boolean haveIndex(Configuration conf, String filePath) throws IOException {
        String path = getIndexFile(conf, filePath);
        FileSystem fs = new Path(path).getFileSystem(conf);

        return fs.exists(new Path(path));
    }

    static Map<String, Object> parseReader(JSONReader jsonReader, CoordinateReferenceSystem crs)
            throws IOException {
        if (null == jsonReader) {
            return null;
        }

        Map<String, Object> pairs = new HashMap<>();
        while (jsonReader.hasNext()) {
            String str = jsonReader.readString();
            if (str.compareToIgnoreCase("bounds") == 0) {
                jsonReader.startArray();
                double minx = jsonReader.readObject(Double.class);
                double maxy = jsonReader.readObject(Double.class);
                double maxx = jsonReader.readObject(Double.class);
                double miny = jsonReader.readObject(Double.class);
                jsonReader.endArray();
                pairs.put("bounds", new ReferencedEnvelope(minx, maxx, miny, maxy, crs));
            } else if (str.compareToIgnoreCase("rows") == 0) {
                pairs.put("rows", jsonReader.readInteger());
            } else if (str.compareToIgnoreCase("cols") == 0) {
                pairs.put("cols", jsonReader.readInteger());
            } else if (str.compareToIgnoreCase("tolerance") == 0) {
                pairs.put("tolerance", jsonReader.readObject(Double.class));
            } else {
                throw new IOException("invalid key: " + str);
            }
        }

        return pairs;
    }

    static CoordinateReferenceSystem getCRS(String crs) {
        CoordinateReferenceSystem referenceSystem = null;
        try {
            referenceSystem = CRS.decode("EPSG:" + Integer.parseInt(crs), true);
        } catch (Exception e) {
            // 可能是wkt，尝试通过wkt解析
            try {
                referenceSystem = CRS.parseWKT(crs);
            } catch (Exception e1) {
                // the wkt might reference an unsupported projection
            }
        }

        return referenceSystem;
    }

    static CoordinateReferenceSystem getCRS(SimpleFeatureType featureType) {
        if (null == featureType) {
            return null;
        }

        return featureType.getCoordinateReferenceSystem();
    }

    static Geometry getQueryGeometry(BoundingBox bounds, Double tolerance) {
        if (null == bounds) {
            return null;
        }

        bounds.setBounds(
                new ReferencedEnvelope(
                        bounds.getMinX() - tolerance,
                        bounds.getMaxX() + tolerance,
                        bounds.getMinY() - tolerance,
                        bounds.getMaxY() + tolerance,
                        null));

        Coordinate[] coordinates =
                new Coordinate[] {
                    new Coordinate(bounds.getMinX(), bounds.getMinY()),
                    new Coordinate(bounds.getMinX(), bounds.getMaxY()),
                    new Coordinate(bounds.getMaxX(), bounds.getMaxY()),
                    new Coordinate(bounds.getMaxX(), bounds.getMinY()),
                    new Coordinate(bounds.getMinX(), bounds.getMinY())
                };

        return JTSFactoryFinder.getGeometryFactory().createPolygon(coordinates);
    }

    /**
     * 获取查询条件
     *
     * @param filterPredicate FilterPredicate
     * @return FilterCompat.NOOP or filter
     */
    static FilterCompat.Filter getFilter(FilterPredicate filterPredicate) {
        if (null == filterPredicate) {
            return FilterCompat.NOOP;
        }

        return FilterCompat.get(filterPredicate);
    }

    // ==== 资源定义 ====
    static final String ID = "_ID_";
    static final String TILES = "_TILES_";
    static final String TIMESTAMP = "_TIMESTAMP_";
    static final String GEOMETRY = "_GEOMETRY_";
    static final String X = "_X_";
    static final String Y = "_Y_";
    static final String MINX = "_MINX_";
    static final String MINY = "_MINY_";
    static final String MAXX = "_MAXX_";
    static final String MAXY = "_MAXY_";

    static final String TYPENAME = "SIMPLEFEATURETYPENAME";
    static final String TYPESPEC = "SIMPLEFEATURETYPESPEC";
    static final String INDEXER = "INDEXER";
    static final String FORMAT = "STORAGEFORMAT";
    static final String SRS = "CRS";
    static final String COMPRESSION = "COMPRESSION";
    static final String VERSION = "VERSION";

    static final String PARTITIONQUADTREE = "com.supermap.bdt.rdd.index.impl.partitionquadtree";
    static final String PARTITIONGRID = "com.supermap.bdt.rdd.index.impl.partitiongrid";
    static final String PARQUET = "PARQUET";
}
