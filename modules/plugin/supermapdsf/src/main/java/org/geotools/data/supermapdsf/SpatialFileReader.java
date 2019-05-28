package org.geotools.data.supermapdsf;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.locationtech.jts.geom.*;
import org.opengis.feature.type.GeometryType;

/** Created by liyq on 2019/3/18. */
public class SpatialFileReader implements Closeable {
    private ByteBuffer byteBuffer = ByteBuffer.allocate(1024);

    private ByteBuffer lengthBytesBuffer = ByteBuffer.allocate(4);

    private GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();

    private FSDataInputStream inStream = null;

    private GeometryType geometryType;

    SpatialFileReader(String path, Configuration conf, GeometryType geometryType)
            throws IOException {
        String lowStr = path.toLowerCase();
        String spatialFile;
        if (lowStr.endsWith(".parquet")) {
            int pos = lowStr.lastIndexOf(".parquet");
            spatialFile = path.substring(0, pos) + ".spatial";
        } else if (lowStr.endsWith(".spatial")) {
            spatialFile = path;
        } else {
            throw new IllegalArgumentException("invalid spatial file path : " + path);
        }

        Path spatialFilePath = new Path(spatialFile);
        FileSystem fileSystem = spatialFilePath.getFileSystem(conf);

        if (fileSystem.exists(spatialFilePath)) {
            inStream = fileSystem.open(spatialFilePath);
        }

        this.geometryType = geometryType;
    }

    public Geometry read(Long pos) throws IOException {
        if (inStream != null && pos >= 0) {
            inStream.seek(pos);
            lengthBytesBuffer.clear();
            inStream.readFully(lengthBytesBuffer.array(), 0, 4);

            int bytesLength = lengthBytesBuffer.getInt();
            if (bytesLength <= byteBuffer.capacity()) {
                byteBuffer.clear();
            } else {
                byteBuffer = ByteBuffer.allocate(bytesLength);
            }
            inStream.readFully(byteBuffer.array(), 0, bytesLength);
            byteBuffer.limit(bytesLength);

            if (geometryType.getBinding() == Point.class) {
                return readPoint(byteBuffer);
            } else if (geometryType.getBinding() == LineString.class) {
                return readLineString(byteBuffer);
            } else if (geometryType.getBinding() == MultiLineString.class) {
                return readMultiLineString(byteBuffer);
            } else if (geometryType.getBinding() == Polygon.class) {
                return readPolygon(byteBuffer);
            } else if (geometryType.getBinding() == MultiPolygon.class) {
                return readMultiPolygon(byteBuffer);
            } else {
                return null;
            }
        }

        return null;
    }

    private Coordinate readCoordinate(ByteBuffer ins) {
        return new Coordinate(ins.getDouble(), ins.getDouble());
    }

    private Point readPoint(ByteBuffer ins) {
        return geometryFactory.createPoint(readCoordinate(ins));
    }

    private LineString readLineString(ByteBuffer ins) {
        int length = ins.getInt();
        Coordinate[] points = new Coordinate[length];
        for (int i = 0; i < length; i++) {
            points[i] = readCoordinate(ins);
        }

        return geometryFactory.createLineString(points);
    }

    private MultiLineString readMultiLineString(ByteBuffer ins) {
        LineString[] subs = new LineString[ins.getInt()];
        for (int i = 0; i < subs.length; i++) {
            subs[i] = readLineString(ins);
        }

        return geometryFactory.createMultiLineString(subs);
    }

    private Coordinate[] readCoordinates(ByteBuffer ins, int num) {
        Coordinate[] points = new Coordinate[num];
        for (int i = 0; i < num; i++) {
            points[i] = readCoordinate(ins);
        }

        return points;
    }

    private Polygon readPolygon(ByteBuffer ins) {
        Coordinate[] outRingPoints = new Coordinate[ins.getInt()];
        for (int i = 0; i < outRingPoints.length; i++) {
            outRingPoints[i] = readCoordinate(ins);
        }

        int inRingsNum = ins.getInt();
        if (inRingsNum > 0) {
            LinearRing[] inRings = new LinearRing[inRingsNum];
            for (int i = 0; i < inRings.length; i++) {
                inRings[i] = geometryFactory.createLinearRing(readCoordinates(ins, ins.getInt()));
            }

            return geometryFactory.createPolygon(
                    geometryFactory.createLinearRing(outRingPoints), inRings);
        } else {
            return geometryFactory.createPolygon(geometryFactory.createLinearRing(outRingPoints));
        }
    }

    private MultiPolygon readMultiPolygon(ByteBuffer ins) {
        int num = ins.getInt();
        Polygon[] polygons = new Polygon[num];
        for (int i = 0; i < num; i++) {
            polygons[i] = readPolygon(ins);
        }

        return geometryFactory.createMultiPolygon(polygons);
    }

    @Override
    public void close() throws IOException {
        if (null != inStream) {
            inStream.close();
            inStream = null;
        }
    }
}
