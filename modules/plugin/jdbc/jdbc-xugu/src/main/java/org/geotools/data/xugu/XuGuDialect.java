package org.geotools.data.xugu;

import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import org.geotools.data.jdbc.FilterToSQL;
import org.geotools.factory.Hints;
import org.geotools.feature.NameImpl;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.jdbc.BasicSQLDialect;
import org.geotools.jdbc.JDBCDataStore;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.GeometryDescriptor;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

/**
 * Created by liyq on 2018/11/1.
 */
public class XuGuDialect extends BasicSQLDialect {

    private Connection connection;

    private int parallel;

    public XuGuDialect(JDBCDataStore dataStore) {
        super(dataStore);
    }

    public Connection getConnection() {
        return this.connection;
    }

    public int getParallel() {
        return parallel;
    }

    public void setParallel(int parallel) {
        this.parallel = parallel;
    }

    public void closeSafe(ResultSet rs) {
        dataStore.closeSafe(rs);
    }

    public void closeSafe(Statement st) {
        dataStore.closeSafe(st);
    }

    @Override
    public void initializeConnection(Connection cx) throws SQLException {
        super.initializeConnection(cx);
        this.connection = cx;
    }

    @Override
    public Class<?> getMapping(ResultSet columnMetaData, Connection cx) throws SQLException {
        String typeName = columnMetaData.getString("TYPE_NAME");
        String colName = columnMetaData.getString("COLUMN_NAME");
        String tableName = columnMetaData.getString("TABLE_NAME");

        if (typeName.equalsIgnoreCase("blob") &&
                colName.equalsIgnoreCase("smgeometry")) {
            String sql =
                    "SELECT SmDataType FROM SmMetaData "
                            + "WHERE upper(SmTableName) = upper("
                            + "'" + tableName + "')";
            Statement st = cx.createStatement();
            ResultSet rs = null;
            try {
                LOGGER.fine(sql);
                rs = st.executeQuery(sql);
                if (rs.next()) {
                    String type = rs.getString("SmDataType");
                    if (type.equalsIgnoreCase("point")) {
                        return Point.class;
                    } else if (type.equalsIgnoreCase("line")) {
                        return MultiLineString.class;
                    } else if (type.equalsIgnoreCase("region")) {
                        return MultiPolygon.class;
                    } else {
                        return null;
                    }
                }
            } finally {
                closeSafe(rs);
                closeSafe(st);
            }
        }

        return super.getMapping(columnMetaData, cx);
    }

    @Override
    public Integer getGeometrySRID(
            String schemaName, String tableName, String columnName, Connection cx)
            throws SQLException {
        String sql =
                "SELECT SmSRID FROM SmMetaData "
                        + "WHERE upper(SmTableName) = upper("
                        + "'" + tableName + "')";
        Statement st = cx.createStatement();
        try {
            ResultSet rs = st.executeQuery(sql);
            try {
                LOGGER.fine(sql);
                if (rs.next()) {
                    return rs.getInt(1);
                }
            } finally {
                closeSafe(rs);
            }
        } finally {
            closeSafe(st);
        }

        return super.getGeometrySRID(schemaName, tableName, columnName, cx);
    }

    @Override
    public FilterToSQL createFilterToSQL() {
        XuGuFilterToSQL sql = new XuGuFilterToSQL(this);
        return sql;
    }

    @Override
    public List<ReferencedEnvelope> getOptimizedBounds(
            String schema, SimpleFeatureType featureType, Connection cx)
            throws SQLException, IOException {
        String tableName = featureType.getTypeName();
        boolean bHasIndex = true;
        if (!dataStore.getNames().contains(new NameImpl(tableName + "_INDEX"))) {
            bHasIndex = false;
        }

        if (dataStore.getVirtualTables().get(tableName) != null) {
            return null;
        }

        GeometryDescriptor att = featureType.getGeometryDescriptor();
        if (null == att) {
            return null;
        }
        Class<?> cls = att.getType().getBinding();

        Statement st = null;
        ResultSet rs = null;
        Savepoint savePoint = null;
        List<ReferencedEnvelope> result = new ArrayList<>();
        try {
            st = cx.createStatement();
            if (!cx.getAutoCommit()) {
                savePoint = cx.setSavepoint();
            }

            String sql;

            if (bHasIndex) {
                tableName += "_INDEX";
            }

            if (cls == Point.class) {
                sql = "SELECT min(SmX) as xmin, max(SmX) as xMax, "
                        + "min(SmY) as yMin, max(SmY) as yMax, "
                        + "FROM" + tableName + " parallel " + parallel;
            } else {
                sql = "SELECT min(SmSdriW) as xMin, max(SmSdriE) as xMax,"
                        + " min(SmSdriS) as yMin, max(SmSdriN) as yMax "
                        + " FROM " + tableName + " parallel " + parallel;
            }

            LOGGER.fine(sql);
            rs = st.executeQuery(sql);

            while (rs.next()) {
                double xMin = rs.getDouble("xMin");
                double xMax = rs.getDouble("xMax");
                double yMin = rs.getDouble("yMin");
                double yMax = rs.getDouble("yMax");
                Envelope env = new Envelope(xMin, xMax, yMin, yMax);

                if (!env.isNull()) {
                    CoordinateReferenceSystem crs = att.getCoordinateReferenceSystem();
                    result.add(new ReferencedEnvelope(env, crs));
                }
            }
        } catch (SQLException e) {
            if (savePoint != null) {
                cx.rollback(savePoint);
            }
            LOGGER.log(Level.WARNING,
                    "Failed to use getBounds"
                            + ", falling back on envelope aggregation", e);
            return null;
        } finally {
            if (savePoint != null) {
                cx.releaseSavepoint(savePoint);
            }

            closeSafe(rs);
            closeSafe(st);
        }

        return result;
    }

    @Override
    public void encodeGeometryValue(
            Geometry value,
            int dimension,
            int srid,
            StringBuffer sql) throws IOException {

    }

    @Override
    public Geometry decodeGeometryValue(
            GeometryDescriptor descriptor,
            ResultSet rs,
            String column,
            GeometryFactory factory,
            Connection cx) throws IOException, SQLException {
        byte bytes[] = rs.getBytes(column);
        if (null == bytes) {
            return null;
        }

        WKBReader wkbReader = new WKBReader(factory);
        try {
            return wkbReader.read(bytes);
        } catch (ParseException e) {
            throw new IOException("Error occurred parsing the WKB", e);
        }
    }

    @Override
    public void encodeGeometryEnvelope(String tableName, String geometryColumn, StringBuffer sql) {
        sql.append(geometryColumn);
    }

    @Override
    public Envelope decodeGeometryEnvelope(ResultSet rs, int column, Connection cx) throws SQLException, IOException {
        byte bytes[] = rs.getBytes(column);
        if (null == bytes) {
            return null;
        }

        WKBReader wkbReader = new WKBReader();
        try {
            Geometry geom = wkbReader.read(bytes);
            if (null != geom) {
                return geom.getEnvelopeInternal();
            } else {
                return new Envelope();
            }
        } catch (ParseException e) {
            throw new IOException("Error occurred parsing the bounds WKB", e);
        }
    }
}
