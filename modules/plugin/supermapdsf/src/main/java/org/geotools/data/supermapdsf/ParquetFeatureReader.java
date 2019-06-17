package org.geotools.data.supermapdsf;

import static org.geotools.data.supermapdsf.SuperMapDSFUtils.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.geotools.data.Query;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.locationtech.jts.geom.*;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;

/** Created by liyq on 2019/3/16. */
public class ParquetFeatureReader extends SuperMapDSFFeatureReader {
    private ParquetReader reader = null;

    private GenericRecord record = null;

    private Geometry geometry = null;

    private boolean done = false;

    private Query query;

    private Iterator<FileStatus> statuses = null;

    ParquetFeatureReader(SimpleFeatureType sft, Query query) {
        this.schema = sft;
        this.query = query;
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

    /**
     * 是否跳过该对象
     *
     * @return true or false
     */
    private boolean isRead() {
        assert (record != null);

        if (query.getFilter() == Filter.INCLUDE) {
            int[] tiles =
                    Arrays.stream(record.get(TILES).toString().split(","))
                            .mapToInt(Integer::valueOf)
                            .toArray();

            if (tiles.length > 0) {
                return Arrays.stream(tiles).min().getAsInt() != currentIndex;
            } else {
                return false;
            }
        } else {
            Integer[] tiles =
                    Arrays.stream(record.get(TILES).toString().split(","))
                            .mapToInt(Integer::valueOf)
                            .boxed()
                            .toArray(Integer[]::new);

            if (tiles.length > 0) {
                return isRead(getIndex(tiles), tiles);
            } else {
                return false;
            }
        }
    }

    @Override
    public boolean hasNext() throws IOException {
        if (null == schema) {
            return false;
        }

        while (!done) {
            if (null != reader && (record = (GenericRecord) reader.read()) != null) {
                if (!isLastedVersion(
                        record.get(ID).toString(),
                        Long.parseLong(record.get(TIMESTAMP).toString()))) {
                    continue;
                }

                if (isRead()) {
                    continue;
                }

                Class<?> geoType = schema.getGeometryDescriptor().getType().getBinding();
                if (geoType == Point.class) {
                    Double dX = Double.parseDouble(record.get(X).toString());
                    Double dY = Double.parseDouble(record.get(Y).toString());
                    geometry =
                            JTSFactoryFinder.getGeometryFactory()
                                    .createPoint(new Coordinate(dX, dY));
                } else if (geoType == LineString.class
                        || geoType == MultiLineString.class
                        || geoType == Polygon.class
                        || geoType == MultiPolygon.class) {
                    geometry =
                            DSFGeometryBuilder.readGeometry(
                                    schema.getGeometryDescriptor().getType(),
                                    getCompress(),
                                    ((ByteBuffer) record.get(GEOMETRY)).array());
                } else {
                    continue;
                }

                done = true;
                continue;
            }

            Configuration conf = new Configuration();
            if (null != statuses && statuses.hasNext()) {
                close();

                FilterToDSF filterToDSF = new FilterToDSF(schema, getTolerance());
                FilterCompat.Filter filter =
                        getFilter((FilterPredicate) query.getFilter().accept(filterToDSF, null));
                reader =
                        AvroParquetReader.builder(
                                        HadoopInputFile.fromPath(statuses.next().getPath(), conf))
                                .useRecordFilter(true)
                                .useSignedStringMinMax(false)
                                .useStatsFilter(true)
                                .withConf(conf)
                                .withFilter(filter)
                                .build();

                readDeleteFeatures();
            } else {
                close();
                if (null != statuses) {
                    statuses = null;
                }

                currentIndex++;
                String rootPath = getPartitionRoot(conf);
                if (null != rootPath && !rootPath.isEmpty()) {
                    Path fPath = new Path(rootPath);
                    FileSystem fs = fPath.getFileSystem(conf);
                    statuses =
                            Arrays.asList(
                                            fs.listStatus(
                                                    fPath,
                                                    path -> path.getName().endsWith(".parquet")))
                                    .iterator();
                } else {
                    return false;
                }
            }
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
