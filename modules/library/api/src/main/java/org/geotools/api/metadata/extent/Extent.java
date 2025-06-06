/*
 *    GeoTools - The Open Source Java GIS Toolkit
 *    http://geotools.org
 *
 *    (C) 2011, Open Source Geospatial Foundation (OSGeo)
 *    (C) 2004-2005, Open Geospatial Consortium Inc.
 *
 *    All Rights Reserved. http://www.opengis.org/legal/
 */
package org.geotools.api.metadata.extent;

import java.util.Collection;
import org.geotools.api.util.InternationalString;

/**
 * Information about spatial, vertical, and temporal extent. This interface has four optional attributes
 * ({@linkplain #getGeographicElements geographic elements}, {@linkplain #getTemporalElements temporal elements}, and
 * {@linkplain #getVerticalElements vertical elements}) and an element called {@linkplain #getDescription description}.
 * At least one of the four shall be used.
 *
 * @version <A HREF="http://www.opengeospatial.org/standards/as#01-111">ISO 19115</A>
 * @author Martin Desruisseaux (IRD)
 * @since GeoAPI 1.0
 */
public interface Extent {
    /**
     * Returns the spatial and temporal extent for the referring object.
     *
     * @return The spatial and temporal extent, or {@code null} in none.
     */
    InternationalString getDescription();

    /**
     * Provides geographic component of the extent of the referring object
     *
     * @return The geographic extent, or an empty set if none.
     */
    Collection<? extends GeographicExtent> getGeographicElements();

    /**
     * Provides temporal component of the extent of the referring object
     *
     * @return The temporal extent, or an empty set if none.
     */
    Collection<? extends TemporalExtent> getTemporalElements();

    /**
     * Provides vertical component of the extent of the referring object
     *
     * @return The vertical extent, or an empty set if none.
     */
    Collection<? extends VerticalExtent> getVerticalElements();
}
