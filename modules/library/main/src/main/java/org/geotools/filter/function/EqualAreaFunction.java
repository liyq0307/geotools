/*
 *    GeoTools - The Open Source Java GIS Toolkit
 *    http://geotools.org
 *
 *    (C) 2018, Open Source Geospatial Foundation (OSGeo)
 *
 *    This library is free software; you can redistribute it and/or
 *    modify it under the terms of the GNU Lesser General Public
 *    License as published by the Free Software Foundation;
 *    version 2.1 of the License.
 *
 *    This library is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *    Lesser General Public License for more details.
 */
package org.geotools.filter.function;

import static org.geotools.filter.capability.FunctionNameImpl.parameter;

import org.geotools.api.filter.FilterFactory;
import org.geotools.api.filter.capability.FunctionName;
import org.geotools.api.filter.expression.Expression;
import org.geotools.api.filter.expression.Function;
import org.geotools.api.filter.expression.Literal;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.visitor.EqualAreaListVisitor;
import org.geotools.feature.visitor.FeatureCalc;
import org.geotools.filter.capability.FunctionNameImpl;

/**
 * Breaks a SimpleFeatureCollection into classes with (roughtly) equal total items area in each class
 *
 * @author Andrea Aime - GeoSolutions
 */
public class EqualAreaFunction extends AbstractQuantityClassificationFunction {

    private static final FilterFactory FF = CommonFactoryFinder.getFilterFactory();

    public static FunctionName NAME = new FunctionNameImpl(
            "EqualArea",
            RangedClassifier.class,
            parameter("value", Double.class),
            parameter("classes", Integer.class),
            parameter("areaFunction", Double.class, 0, 1),
            parameter("percentages", Boolean.class, 0, 1));

    public EqualAreaFunction() {
        super(NAME);
    }

    /**
     * The default area is computed as a cartesian area of the data (will work reasonably on geodetic dataset over small
     * areas, but won't work properly over large areas) However, it is to be remembered that these classification
     * functions are trying to get a certain evennes on the display, so if the display is in plate caree, then computing
     * area over lon/lat is actually the right thing to do.
     */
    public static Function getCartesianAreaFunction() {
        // Would have loved to keep this as static, but cannot be done since the equal area
        // function class is created while the function lookup is being initialized
        return FF.function("area2", FF.property(""));
    }

    @Override
    protected FeatureCalc getListVisitor() {
        Expression areaFunction = getEqualAreaParameter();
        if (areaFunction == null) {
            areaFunction = getCartesianAreaFunction();
        }
        return new EqualAreaListVisitor(getParameters().get(0), areaFunction, getClasses());
    }

    @Override
    protected boolean percentages() {
        if (getParameters().size() > 3) {
            Object value = ((Literal) getParameters().get(3)).getValue();
            if (value instanceof Boolean) return ((Boolean) value).booleanValue();
        }
        return false;
    }

    private Literal getEqualAreaParameter() {
        if (getParameters().size() > 2) {
            Literal literal = (Literal) getParameters().get(2);
            if (literal.getValue() instanceof Double) return literal;
        }
        return null;
    }
}
