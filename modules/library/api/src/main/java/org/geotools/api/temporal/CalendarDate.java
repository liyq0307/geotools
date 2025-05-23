/*
 *    GeoTools - The Open Source Java GIS Toolkit
 *    http://geotools.org
 *
 *    (C) 2011, Open Source Geospatial Foundation (OSGeo)
 *    (C) 2005, Open Geospatial Consortium Inc.
 *
 *    All Rights Reserved. http://www.opengis.org/legal/
 */
package org.geotools.api.temporal;

import org.geotools.api.util.InternationalString;

/**
 * A data type that shall be used to identify temporal position within a calendar.
 *
 * @author Stephane Fellah (Image Matters)
 * @author Alexander Petkov
 */
public interface CalendarDate extends TemporalPosition {
    /** Provides the name of the {@linkplain CalendarEra calendar era} to which the date is referenced. */
    InternationalString getCalendarEraName();

    /**
     * Provides a sequence of integers in which the first integer identifies a specific instance of the unit used at the
     * highest level of the calendar hierarchy, the second integer identifies a specific instance of the unit used at
     * the next lower level in the hierarchy, and so on. The format defined in ISO 8601 for dates in the Gregorian
     * calendar may be used for any date that is composed of values for year, month and day.
     *
     * @todo Should we returns an array of some primitive type instead?
     */
    int[] getCalendarDate();
}
