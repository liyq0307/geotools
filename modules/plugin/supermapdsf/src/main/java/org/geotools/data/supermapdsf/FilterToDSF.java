package org.geotools.data.supermapdsf;

import static org.geotools.data.supermapdsf.SuperMapDSFUtils.*;

import java.util.*;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.io.api.Binary;
import org.geotools.filter.LiteralExpressionImpl;
import org.locationtech.jts.geom.*;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.filter.*;
import org.opengis.filter.expression.*;
import org.opengis.filter.identity.Identifier;
import org.opengis.filter.spatial.*;
import org.opengis.filter.temporal.*;
import org.opengis.geometry.BoundingBox;

/** Created by liyq on 2019/6/10 17:11. */
public class FilterToDSF implements FilterVisitor, ExpressionVisitor {

    private SimpleFeatureType featureType;

    private double tolerance;

    FilterToDSF(SimpleFeatureType sft, double tolerance) {
        this.featureType = sft;
        this.tolerance = tolerance;
    }

    @Override
    public Object visitNullFilter(Object extraData) {
        return null;
    }

    @Override
    public Object visit(ExcludeFilter filter, Object extraData) {
        return null;
    }

    @Override
    public Object visit(IncludeFilter filter, Object extraData) {
        return null;
    }

    @Override
    public Object visit(PropertyIsBetween filter, Object extraData) {
        Expression expr = filter.getExpression();
        Expression lowerBounds = filter.getLowerBoundary();
        Expression upperBounds = filter.getUpperBoundary();

        Class context;
        AttributeDescriptor attType = (AttributeDescriptor) expr.evaluate(featureType);
        if (attType != null) {
            context = attType.getType().getBinding();
        } else {
            context = String.class;
        }

        String name = (String) expr.accept(this, extraData);
        FilterPredicate left;
        FilterPredicate right;
        Object lower = lowerBounds.accept(this, context);
        Object upper = upperBounds.accept(this, context);
        if (context == Integer.class) {
            left = FilterApi.gtEq(FilterApi.intColumn(name), (Integer) lower);
            right = FilterApi.ltEq(FilterApi.intColumn(name), (Integer) upper);
        } else if (context == Long.class) {
            left = FilterApi.gtEq(FilterApi.longColumn(name), (Long) lower);
            right = FilterApi.ltEq(FilterApi.longColumn(name), (Long) upper);
        } else if (context == double.class) {
            left = FilterApi.gtEq(FilterApi.doubleColumn(name), (double) lower);
            right = FilterApi.ltEq(FilterApi.doubleColumn(name), (double) upper);
        } else if (context == float.class) {
            left = FilterApi.gtEq(FilterApi.floatColumn(name), (float) lower);
            right = FilterApi.ltEq(FilterApi.floatColumn(name), (float) upper);
        } else if (context == String.class) {
            left =
                    FilterApi.gtEq(
                            FilterApi.binaryColumn(name), Binary.fromString(lower.toString()));
            right =
                    FilterApi.ltEq(
                            FilterApi.binaryColumn(name), Binary.fromString(upper.toString()));
        } else {
            return null;
        }

        return FilterApi.and(left, right);
    }

    private List<Object> visit(BinaryLogicOperator filter, Object extraData) {
        Iterator<Filter> list = filter.getChildren().iterator();
        List<Object> childs = new ArrayList<>();
        while (list.hasNext()) {
            Filter childFilter = list.next();
            childs.add(childFilter.accept(this, extraData));
        }

        return childs;
    }

    private Object reduceLeft(List<Object> values, Object extraData) {
        boolean first = true;
        Object acc = null;
        for (Object value : values) {
            if (first) {
                acc = value;
                first = false;
            } else {
                switch (extraData.toString().toUpperCase()) {
                    case "OR":
                        acc = FilterApi.or((FilterPredicate) acc, (FilterPredicate) value);
                        break;
                    case "AND":
                        acc = FilterApi.and((FilterPredicate) acc, (FilterPredicate) value);
                        break;
                    default:
                        break;
                }
            }
        }

        return acc;
    }

    @Override
    public Object visit(And filter, Object extraData) {
        List<Object> values = visit((BinaryLogicOperator) filter, "AND");
        return reduceLeft(values, "AND");
    }

    @Override
    public Object visit(Or filter, Object extraData) {
        List<Object> values = visit((BinaryLogicOperator) filter, "OR");
        return reduceLeft(values, "OR");
    }

    @Override
    public Object visit(Id filter, Object extraData) {
        Set<Identifier> ids = filter.getIdentifiers();
        List<Object> newIds = new ArrayList<>();
        for (Identifier id : ids) {
            newIds.add(FilterApi.eq(FilterApi.binaryColumn(ID), Binary.fromString(id.toString())));
        }

        return reduceLeft(newIds, "OR");
    }

    @Override
    public Object visit(Not not, Object extraData) {
        Filter filter = not.getFilter();
        Object value = filter.accept(this, extraData);
        if (null != value) {
            return FilterApi.not((FilterPredicate) value);
        }

        return null;
    }

    private Object visit(BinaryComparisonOperator filter, Object extraData)
            throws RuntimeException {
        Expression left = filter.getExpression1();
        Expression right = filter.getExpression2();

        // see if we can get some indication on how to evaluate literals
        Class attType = null, leftContext = null, rightContext = null;
        if (left instanceof PropertyName) {
            AttributeDescriptor descriptor = (AttributeDescriptor) left.evaluate(featureType);
            if (descriptor != null) {
                rightContext = descriptor.getType().getBinding();
                attType = rightContext;
            }
        }

        if (rightContext == null && right instanceof PropertyName) {
            AttributeDescriptor descriptor = (AttributeDescriptor) right.evaluate(featureType);
            if (descriptor != null) {
                leftContext = descriptor.getType().getBinding();
                attType = leftContext;
            }
        }

        String type = ((String) extraData).toUpperCase();
        Object lf = left.accept(this, leftContext);
        Object rg = right.accept(this, rightContext);
        if (attType == Integer.class) {
            switch (type) {
                case ">":
                    return FilterApi.gt(FilterApi.intColumn(lf.toString()), (Integer) rg);
                case ">=":
                    return FilterApi.gtEq(FilterApi.intColumn(lf.toString()), (Integer) rg);
                case "<":
                    return FilterApi.lt(FilterApi.intColumn(lf.toString()), (Integer) rg);
                case "<=":
                    return FilterApi.ltEq(FilterApi.intColumn(lf.toString()), (Integer) rg);
                case "=":
                    return FilterApi.eq(FilterApi.intColumn(lf.toString()), (Integer) rg);
                case "!=":
                    return FilterApi.notEq(FilterApi.intColumn(lf.toString()), (Integer) rg);
            }
        } else if (attType == Long.class) {
            switch (type) {
                case ">":
                    return FilterApi.gt(FilterApi.longColumn(lf.toString()), (Long) rg);
                case ">=":
                    return FilterApi.gtEq(FilterApi.longColumn(lf.toString()), (Long) rg);
                case "<":
                    return FilterApi.lt(FilterApi.longColumn(lf.toString()), (Long) rg);
                case "<=":
                    return FilterApi.ltEq(FilterApi.longColumn(lf.toString()), (Long) rg);
                case "=":
                    return FilterApi.eq(FilterApi.longColumn(lf.toString()), (Long) rg);
                case "!=":
                    return FilterApi.notEq(FilterApi.longColumn(lf.toString()), (Long) rg);
            }
        } else if (attType == Double.class) {
            switch (type) {
                case ">":
                    return FilterApi.gt(FilterApi.doubleColumn(lf.toString()), (Double) rg);
                case ">=":
                    return FilterApi.gtEq(FilterApi.doubleColumn(lf.toString()), (Double) rg);
                case "<":
                    return FilterApi.lt(FilterApi.doubleColumn(lf.toString()), (Double) rg);
                case "<=":
                    return FilterApi.ltEq(FilterApi.doubleColumn(lf.toString()), (Double) rg);
                case "=":
                    return FilterApi.eq(FilterApi.doubleColumn(lf.toString()), (Double) rg);
                case "!=":
                    return FilterApi.notEq(FilterApi.doubleColumn(lf.toString()), (Double) rg);
            }
        } else if (attType == Float.class) {
            switch (type) {
                case ">":
                    return FilterApi.gt(FilterApi.floatColumn(lf.toString()), (Float) rg);
                case ">=":
                    return FilterApi.gtEq(FilterApi.floatColumn(lf.toString()), (Float) rg);
                case "<":
                    return FilterApi.lt(FilterApi.floatColumn(lf.toString()), (Float) rg);
                case "<=":
                    return FilterApi.ltEq(FilterApi.floatColumn(lf.toString()), (Float) rg);
                case "=":
                    return FilterApi.eq(FilterApi.floatColumn(lf.toString()), (Float) rg);
                case "!=":
                    return FilterApi.notEq(FilterApi.floatColumn(lf.toString()), (Float) rg);
            }
        } else if (attType == String.class) {
            switch (type) {
                case ">":
                    return FilterApi.gt(
                            FilterApi.binaryColumn(lf.toString()),
                            Binary.fromString(rg.toString()));
                case ">=":
                    return FilterApi.gtEq(
                            FilterApi.binaryColumn(lf.toString()),
                            Binary.fromString(rg.toString()));
                case "<":
                    return FilterApi.lt(
                            FilterApi.binaryColumn(lf.toString()),
                            Binary.fromString(rg.toString()));
                case "<=":
                    return FilterApi.ltEq(
                            FilterApi.binaryColumn(lf.toString()),
                            Binary.fromString(rg.toString()));
                case "=":
                    return FilterApi.eq(
                            FilterApi.binaryColumn(lf.toString()),
                            Binary.fromString(rg.toString()));
                case "!=":
                    return FilterApi.notEq(
                            FilterApi.binaryColumn(lf.toString()),
                            Binary.fromString(rg.toString()));
            }
        } else if (attType == boolean.class) {
            switch (type) {
                case "=":
                    return FilterApi.eq(
                            FilterApi.booleanColumn(lf.toString()),
                            rg.toString().compareToIgnoreCase("true") == 0);
            }
        }

        return null;
    }

    @Override
    public Object visit(PropertyIsEqualTo filter, Object extraData) {
        return visit((BinaryComparisonOperator) filter, "=");
    }

    @Override
    public Object visit(PropertyIsNotEqualTo filter, Object extraData) {
        return visit((BinaryComparisonOperator) filter, "!=");
    }

    @Override
    public Object visit(PropertyIsGreaterThan filter, Object extraData) {
        return visit((BinaryComparisonOperator) filter, ">");
    }

    @Override
    public Object visit(PropertyIsGreaterThanOrEqualTo filter, Object extraData) {
        return visit((BinaryComparisonOperator) filter, ">=");
    }

    @Override
    public Object visit(PropertyIsLessThan filter, Object extraData) {
        return visit((BinaryComparisonOperator) filter, "<");
    }

    @Override
    public Object visit(PropertyIsLessThanOrEqualTo filter, Object extraData) {
        return visit((BinaryComparisonOperator) filter, "<=");
    }

    @Override
    public Object visit(Literal expression, Object extraData) {
        Class target = null;
        if (extraData instanceof Class) {
            target = (Class) extraData;
        }

        // evaluate the expression
        Object literal = null;
        if (target != null) {
            literal = expression.evaluate(null, target);
        }

        if (literal == null) {
            literal = expression.getValue();
        }

        return literal;
    }

    @Override
    public Object visit(PropertyName expression, Object extraData) {
        AttributeDescriptor attribute = null;
        try {
            attribute = (AttributeDescriptor) expression.evaluate(featureType);
        } catch (Exception e) {
            String msg = "Error occured mapping " + expression + " to feature type";
            System.out.println(msg);
        }

        String name;
        if (attribute != null) {
            name = attribute.getLocalName();
        } else {
            name = expression.getPropertyName();
        }

        return name;
    }

    private Object visitSpatial(Envelope envelope, String type) {
        if (null == envelope) {
            return null;
        }

        if (type.toUpperCase().equals("BBOX") || type.toUpperCase().equals("INTERSECTS")) {
            Class<?> classZ = featureType.getGeometryDescriptor().getType().getBinding();
            if (classZ == Point.class) {
                FilterPredicate LR =
                        FilterApi.and(
                                FilterApi.gt(FilterApi.doubleColumn(X), envelope.getMinX()),
                                FilterApi.lt(FilterApi.doubleColumn(X), envelope.getMaxX()));
                FilterPredicate BT =
                        FilterApi.and(
                                FilterApi.gt(FilterApi.doubleColumn(Y), envelope.getMinX()),
                                FilterApi.lt(FilterApi.doubleColumn(Y), envelope.getMaxX()));
                return FilterApi.and(LR, BT);
            } else if (classZ == LineString.class
                    || classZ == Polygon.class
                    || classZ == MultiLineString.class
                    || classZ == MultiPolygon.class) {
                FilterPredicate LR =
                        FilterApi.and(
                                FilterApi.gt(FilterApi.doubleColumn(MAXX), envelope.getMinX()),
                                FilterApi.lt(FilterApi.doubleColumn(MINX), envelope.getMaxX()));
                FilterPredicate BT =
                        FilterApi.and(
                                FilterApi.gt(FilterApi.doubleColumn(MAXY), envelope.getMinY()),
                                FilterApi.lt(FilterApi.doubleColumn(MINY), envelope.getMaxY()));
                return FilterApi.and(LR, BT);
            }
        }

        return null;
    }

    @Override
    public Object visit(BBOX filter, Object extraData) {
        if (filter.getExpression1() instanceof PropertyName) {
            if (!((PropertyName) filter.getExpression1())
                    .getPropertyName()
                    .equals(featureType.getGeometryDescriptor().getLocalName())) {
                return null;
            }
        } else {
            return null;
        }

        BoundingBox bounds = filter.getBounds();
        if (null == bounds) {
            return null;
        }

        Geometry queryGeometry = getQueryGeometry(bounds, tolerance);
        if (null == queryGeometry) {
            return null;
        }

        return visitSpatial(queryGeometry.getEnvelopeInternal(), filter.NAME);
    }

    @Override
    public Object visit(Intersects filter, Object extraData) {
        Expression exp1 = filter.getExpression1();
        Expression exp2 = filter.getExpression2();

        Geometry jtsGeometry;
        if (exp1 instanceof LiteralExpressionImpl) {
            jtsGeometry = exp1.evaluate(null, Geometry.class);
        } else {
            jtsGeometry = exp2.evaluate(null, Geometry.class);
        }

        if (null == jtsGeometry) {
            return null;
        }

        return visitSpatial(jtsGeometry.getEnvelopeInternal(), filter.NAME);
    }

    // ================================= 不支持的类型 ======================
    @Override
    public Object visit(PropertyIsLike filter, Object extraData) {
        return null;
    }

    @Override
    public Object visit(PropertyIsNull filter, Object extraData) {
        return null;
    }

    @Override
    public Object visit(PropertyIsNil filter, Object extraData) {
        return null;
    }

    @Override
    public Object visit(Contains filter, Object extraData) {
        return null;
    }

    @Override
    public Object visit(Beyond filter, Object extraData) {
        return null;
    }

    @Override
    public Object visit(Crosses filter, Object extraData) {
        return null;
    }

    @Override
    public Object visit(Disjoint filter, Object extraData) {
        return null;
    }

    @Override
    public Object visit(DWithin filter, Object extraData) {
        return null;
    }

    @Override
    public Object visit(Equals filter, Object extraData) {
        return null;
    }

    @Override
    public Object visit(Overlaps filter, Object extraData) {
        return null;
    }

    @Override
    public Object visit(Touches filter, Object extraData) {
        return null;
    }

    @Override
    public Object visit(Within filter, Object extraData) {
        return null;
    }

    @Override
    public Object visit(After after, Object extraData) {
        return null;
    }

    @Override
    public Object visit(AnyInteracts anyInteracts, Object extraData) {
        return null;
    }

    @Override
    public Object visit(Before before, Object extraData) {
        return null;
    }

    @Override
    public Object visit(Begins begins, Object extraData) {
        return null;
    }

    @Override
    public Object visit(BegunBy begunBy, Object extraData) {
        return null;
    }

    @Override
    public Object visit(During during, Object extraData) {
        return null;
    }

    @Override
    public Object visit(EndedBy endedBy, Object extraData) {
        return null;
    }

    @Override
    public Object visit(Ends ends, Object extraData) {
        return null;
    }

    @Override
    public Object visit(Meets meets, Object extraData) {
        return null;
    }

    @Override
    public Object visit(MetBy metBy, Object extraData) {
        return null;
    }

    @Override
    public Object visit(OverlappedBy overlappedBy, Object extraData) {
        return null;
    }

    @Override
    public Object visit(TContains contains, Object extraData) {
        return null;
    }

    @Override
    public Object visit(TEquals equals, Object extraData) {
        return null;
    }

    @Override
    public Object visit(TOverlaps contains, Object extraData) {
        return null;
    }

    @Override
    public Object visit(NilExpression expression, Object extraData) {
        return null;
    }

    @Override
    public Object visit(Add expression, Object extraData) {
        return null;
    }

    @Override
    public Object visit(Divide expression, Object extraData) {
        return null;
    }

    @Override
    public Object visit(Function expression, Object extraData) {
        return null;
    }

    @Override
    public Object visit(Multiply expression, Object extraData) {
        return null;
    }

    @Override
    public Object visit(Subtract expression, Object extraData) {
        return null;
    }
}
