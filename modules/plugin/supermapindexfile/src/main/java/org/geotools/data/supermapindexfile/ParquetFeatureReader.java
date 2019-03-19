package org.geotools.data.supermapindexfile;

import static org.geotools.data.supermapindexfile.SuperMapIndexFileUtils.*;

import java.io.IOException;
import java.util.NoSuchElementException;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.locationtech.jts.geom.*;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

/** Created by liyq on 2019/3/16. */
public class ParquetFeatureReader extends SuperMapIndexFileFeatureReader {
    private ParquetReader reader = null;

    private SpatialFileReader spatialReader = null;

    private GenericRecord record = null;

    private Geometry geometry = null;

    private boolean done = false;

    ParquetFeatureReader(String className, String fileDirectory, SimpleFeatureType sft)
            throws IOException {
        this.className = className;
        this.schema = sft;
        this.fileDirectory = fileDirectory;
    }

    @Override
    public SimpleFeature next()
            throws IOException, IllegalArgumentException, NoSuchElementException {
        if (hasNext()) {
            SimpleFeature feature = buildFeature(record, geometry);
            done = false;
            return feature;
        }

        return null;
    }

    @Override
    public boolean hasNext() throws IOException {
        if (null == schema) {
            return false;
        }

        while (!done) {
            if (null != reader) {
                record = (GenericRecord) reader.read();
                if (null == record) {
                    close();
                    currentIndex++;
                    continue;
                }

                String[] tiles = record.get(TILES).toString().split(",");
                if (tiles.length > 0) {
                    Integer[] newPartNumbers = new Integer[tiles.length];
                    for (int i = 0; i < tiles.length; i++) {
                        newPartNumbers[i] = Integer.parseInt(tiles[i]);
                    }

                    if (isRead(getIndex(newPartNumbers), newPartNumbers)) {
                        continue;
                    }
                }

                Class<?> geoType = schema.getGeometryDescriptor().getType().getBinding();
                if (geoType == Point.class) {
                    Double dX = Double.parseDouble(record.get(X).toString());
                    Double dY = Double.parseDouble(record.get(Y).toString());
                    geometry =
                            JTSFactoryFinder.getGeometryFactory()
                                    .createPoint(new Coordinate(dX, dY));
                } else if (geoType == MultiLineString.class || geoType == MultiPolygon.class) {
                    Long nPos = Long.valueOf(String.valueOf(record.get(GEOMETRYPOSITION)));
                    geometry = spatialReader.read(nPos);
                } else {
                    continue;
                }

                if (!getPartGeoIsAllBBox().get(currentIndex)
                        && !getQueryGeometry().intersects(geometry)) {
                    continue;
                }

                done = true;
                continue;
            }

            String partFile = getPartFile(".parquet");
            if (partFile.isEmpty()) {
                return false;
            }

            Configuration conf = new Configuration();
            reader = AvroParquetReader.builder(new Path(partFile)).withConf(conf).build();
            spatialReader =
                    new SpatialFileReader(partFile, conf, schema.getGeometryDescriptor().getType());
        }

        return true;
    }

    @Override
    public void close() throws IOException {
        if (null != reader) {
            reader.close();
            reader = null;
        }

        if (null != spatialReader) {
            spatialReader.close();
            spatialReader = null;
        }
    }
}
