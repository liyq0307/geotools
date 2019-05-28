package org.geotools.data.supermapdsf;

import static org.geotools.data.supermapdsf.SuperMapDSFFileUtils.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

/** Created by liyq on 2019/3/16. */
public class AvroFeatureReader extends SuperMapDSFFeatureReader {

    private GenericRecord record = null;

    private Geometry geometry = null;

    private FileReader<GenericRecord> reader = null;

    private boolean done = false;

    AvroFeatureReader(String className, String fileDirectory, SimpleFeatureType sft) {
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
            // 使用了需要重新获取
            if (reader != null) {
                if (!reader.hasNext()) {
                    close();
                    currentIndex++;
                    continue;
                }

                // 存在
                record = reader.next(record);
                String[] tiles = record.get(TILES).toString().split(",");
                if (tiles != null && tiles.length > 0) {
                    Integer[] newPartNumbers = new Integer[tiles.length];
                    for (int i = 0; i < tiles.length; i++) {
                        newPartNumbers[i] = Integer.parseInt(tiles[i]);
                    }

                    if (isRead(getIndex(newPartNumbers), newPartNumbers)) {
                        continue;
                    }
                }

                // 取几何对象
                Class geoType = schema.getGeometryDescriptor().getType().getBinding();
                if (geoType == Point.class) {
                    Double dX = Double.parseDouble(record.get(X).toString());
                    Double dY = Double.parseDouble(record.get(Y).toString());
                    geometry =
                            JTSFactoryFinder.getGeometryFactory()
                                    .createPoint(new Coordinate(dX, dY));
                } else if (geoType == MultiLineString.class || geoType == MultiPolygon.class) {
                    try {
                        geometry =
                                new WKBReader().read(((ByteBuffer) record.get(GEOMETRY)).array());
                    } catch (ParseException e) {
                        throw new IOException("Unknown WKB");
                    }
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

            String partFile = getPartFile(".avro");
            if (partFile.isEmpty()) {
                return false;
            }

            Configuration conf = new Configuration();
            FsInput fsInput = new FsInput(new Path(partFile), conf);
            reader = DataFileReader.openReader(fsInput, new GenericDatumReader<>());
        }

        return true;
    }

    @Override
    public void close() throws IOException {
        if (null != reader) {
            reader.close();
            reader = null;
        }
    }
}
