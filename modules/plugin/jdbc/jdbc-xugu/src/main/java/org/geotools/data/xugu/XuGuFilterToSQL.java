package org.geotools.data.xugu;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;
import org.geotools.data.jdbc.FilterToSQL;
import org.geotools.filter.FilterCapabilities;
import org.geotools.filter.LiteralExpressionImpl;
import org.opengis.feature.type.GeometryDescriptor;
import org.opengis.filter.expression.Expression;
import org.opengis.filter.expression.Literal;
import org.opengis.filter.expression.PropertyName;
import org.opengis.filter.spatial.BBOX;
import org.opengis.filter.spatial.BinarySpatialOperator;
import org.opengis.filter.spatial.Intersects;
import org.opengis.geometry.BoundingBox;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by liyq on 2018/11/2.
 */
public class XuGuFilterToSQL extends FilterToSQL {

    private XuGuDialect xuGuDialect;

    public XuGuFilterToSQL(XuGuDialect xuGuDialect) {
        this.xuGuDialect = xuGuDialect;
    }

    private XuGuDialect getDialect() {
        return this.xuGuDialect;
    }

    @SuppressWarnings("deprecation")
    @Override
    protected FilterCapabilities createFilterCapabilities() {
        FilterCapabilities caps = super.createFilterCapabilities();
        caps.addType(BBOX.class);
        caps.addType(Intersects.class);

        return caps;
    }

    @Override
    protected Object visitBinarySpatialOperator(
            BinarySpatialOperator filter,
            PropertyName property,
            Literal geometry,
            boolean swapped,
            Object extraData) {
        XuGuDialect dialect = getDialect();
        Connection cx = dialect.getConnection();
        if (null == cx) {
            return null;
        }

        double minx, maxx, miny, maxy;
        String boxSQL;
        ResultSet rs = null;
        Statement st = null;

        String typeName = featureType.getTypeName();
        GeometryDescriptor att = featureType.getGeometryDescriptor();
        Class<?> cls = att.getType().getBinding();
        int parallel = dialect.getParallel();

        if (filter instanceof BBOX) {
            BoundingBox bBox = ((BBOX) filter).getBounds();
            minx = bBox.getMinX();
            maxx = bBox.getMaxX();
            miny = bBox.getMinY();
            maxy = bBox.getMaxY();

            if (cls == Point.class) {
                boxSQL = " AND SmX > " + minx
                        + " AND SmX < " + maxx
                        + " AND SmY > " + miny
                        + " AND SmX < " + maxy
                        + " parallel " + parallel;
            } else {
                boxSQL = " AND SmSdriW > " + minx
                        + " AND SmSdriE < " + maxx
                        + " AND SmSdriS > " + miny
                        + " AND SmSdriN < " + maxy
                        + " parallel " + parallel;
            }
        } else if (filter instanceof Intersects) {
            Expression exp1 = filter.getExpression1();
            Expression exp2 = filter.getExpression2();

            Geometry jtsGeometry = null;
            if (exp1 instanceof LiteralExpressionImpl) {
                jtsGeometry = exp1.evaluate(null, Geometry.class);
            } else {
                jtsGeometry = exp2.evaluate(null, Geometry.class);
            }

            if (null == jtsGeometry) {
                return null;
            }

            Envelope envelope = jtsGeometry.getEnvelopeInternal();
            minx = envelope.getMinX();
            maxx = envelope.getMaxX();
            miny = envelope.getMinY();
            maxy = envelope.getMaxY();

            if (cls == Point.class) {
                boxSQL = " AND SmX > " + minx
                        + " AND SmX < " + maxx
                        + " AND SmY > " + miny
                        + " AND SmY < " + maxy
                        + " parallel " + parallel;
            } else {
                boxSQL = " AND SmSdriE > " + minx
                        + " AND SmSdriW < " + maxx
                        + " AND SmSdriN > " + miny
                        + " AND SmSdriS < " + maxy
                        + " parallel " + parallel;
            }
        } else {
            return null;
        }

        try {
            String sql =
                    "SELECT SmGridIndexID FROM " + typeName + "_INDEX WHERE "
                            + "SmSdriW < " + maxx + " AND SmSdriE > " + minx
                            + "AND SmSdriN > " + miny + " AND SmSdriS < " + maxy;

            st = cx.createStatement();
            rs = st.executeQuery(sql);

            StringBuilder sb = new StringBuilder();
            sb.append("SmGridIndexID IN (");
            if (!rs.next()) {
                return null;
            }
            sb.append(rs.getInt(1));
            sb.append(",");

            while (rs.next()) {
                sb.append(rs.getInt(1));
                sb.append(",");
            }
            sb.deleteCharAt(sb.length() - 1);
            sb.append(")");
            sb.append(boxSQL);

            out.write(sb.toString());
        } catch (SQLException | IOException e) {
            e.printStackTrace();
        } finally {
            xuGuDialect.closeSafe(rs);
            xuGuDialect.closeSafe(st);
        }

        return extraData;
    }
}
