/*
 *    GeoTools - The Open Source Java GIS Toolkit
 *    http://geotools.org
 *
 *    (C) 2011, Open Source Geospatial Foundation (OSGeo)
 *    (C) 2005 Open Geospatial Consortium Inc.
 *
 *    All Rights Reserved. http://www.opengis.org/legal/
 */
package org.geotools.api.filter;

// Annotations

/**
 * Filter operator that checks that its first sub-expression is greater or equal to its second subexpression.
 *
 * @version <A HREF="http://www.opengis.org/docs/02-059.pdf">Implementation specification 1.0</A>
 * @author Chris Dillard (SYS Technologies)
 * @since GeoAPI 2.0
 */
public interface PropertyIsGreaterThanOrEqualTo extends BinaryComparisonOperator {
    /** Operator name used to check FilterCapabilities */
    public static String NAME = "GreaterThanEqualTo";
}
