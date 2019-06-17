package org.geotools.data.supermapdsf;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.locationtech.jts.geom.*;
import org.opengis.feature.type.GeometryType;

/** Created by liyq on 2019/3/18. */
public class DSFGeometryBuilder implements Closeable {
    private static GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();

    private static byte[] compress(String compressName, byte[] bytes) {
        switch (compressName.toLowerCase()) {
            case "snappy":
                {
                    try {
                        return org.xerial.snappy.Snappy.compress(bytes);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            case "lz4":
                {
                    LZ4Compressor lz4Compressor = LZ4Factory.nativeInstance().fastCompressor();
                    byte[] compBytes = lz4Compressor.compress(bytes);
                    ByteBuffer byteBuffer = ByteBuffer.allocate(4 + compBytes.length);
                    byteBuffer.putInt(bytes.length);
                    byteBuffer.put(compBytes, 4, compBytes.length);
                    return byteBuffer.array();
                }
            default:
                {
                    return bytes;
                }
        }
    }

    private static byte[] decompress(String compressName, byte[] bytes) {
        switch (compressName.toLowerCase()) {
            case "snappy":
                {
                    try {
                        return org.xerial.snappy.Snappy.uncompress(bytes);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            case "lz4":
                {
                    int maxLength = ByteBuffer.wrap(bytes, 0, 4).getInt();
                    LZ4FastDecompressor lz4Compressor =
                            LZ4Factory.nativeInstance().fastDecompressor();
                    return lz4Compressor.decompress(bytes, 4, maxLength);
                }
            default:
                {
                    return bytes;
                }
        }
    }

    static Geometry readGeometry(GeometryType geometryType, String compress, byte[] bytes) {
        byte[] unCompressBytes = decompress(compress, bytes);
        ByteBuffer byteBuffer = ByteBuffer.wrap(unCompressBytes);

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

    private static Coordinate readCoordinate(ByteBuffer ins) {
        return new Coordinate(ins.getDouble(), ins.getDouble());
    }

    private static Point readPoint(ByteBuffer ins) {
        return geometryFactory.createPoint(readCoordinate(ins));
    }

    private static LineString readLineString(ByteBuffer ins) {
        int length = ins.getInt();
        Coordinate[] points = new Coordinate[length];
        for (int i = 0; i < length; i++) {
            points[i] = readCoordinate(ins);
        }

        return geometryFactory.createLineString(points);
    }

    private static MultiLineString readMultiLineString(ByteBuffer ins) {
        LineString[] subs = new LineString[ins.getInt()];
        for (int i = 0; i < subs.length; i++) {
            subs[i] = readLineString(ins);
        }

        return geometryFactory.createMultiLineString(subs);
    }

    private static Coordinate[] readCoordinates(ByteBuffer ins, int num) {
        Coordinate[] points = new Coordinate[num];
        for (int i = 0; i < num; i++) {
            points[i] = readCoordinate(ins);
        }

        return points;
    }

    private static Polygon readPolygon(ByteBuffer ins) {
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

    private static MultiPolygon readMultiPolygon(ByteBuffer ins) {
        int num = ins.getInt();
        Polygon[] polygons = new Polygon[num];
        for (int i = 0; i < num; i++) {
            polygons[i] = readPolygon(ins);
        }

        return geometryFactory.createMultiPolygon(polygons);
    }

    @Override
    public void close() {}
}
