/*
 *    GeoTools - The Open Source Java GIS Toolkit
 *    http://geotools.org
 *
 *    (C) 2004-2008, Open Source Geospatial Foundation (OSGeo)
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
package org.geotools.xml.handlers;

import java.net.URI;
import java.util.Map;
import org.geotools.xml.XMLElementHandler;
import org.geotools.xml.schema.Element;
import org.xml.sax.Attributes;

/**
 * This is a default Handler which is used in case a handler cannot be created for an arbitry element. Note: this
 * handler does not perform and parsing on the current element or it's children.
 *
 * @author dzwiers, Refractions Research, Inc. http://www.refractions.net
 * @author $Author:$ (last modification)
 * @version $Id$
 */
public class IgnoreHandler extends XMLElementHandler {

    public static final String NAME = "IGNORE_HANDLER";

    private Element elem;

    public IgnoreHandler() {
        elem = null;
    }

    public IgnoreHandler(Element igElem) {
        elem = igElem;
    }

    /** @see org.geotools.xml.XMLElementHandler#getElement() */
    @Override
    public Element getElement() {
        return elem;
    }

    /** @see org.geotools.xml.XMLElementHandler#characters(java.lang.String) */
    @Override
    public void characters(String text) {
        // do nothing
    }

    /** @see schema.XSIElementHandler#getHandler(java.lang.String, java.lang.String) */
    @Override
    public XMLElementHandler getHandler(URI namespaceURI, String localName, Map<String, Object> hints) {
        return this;
    }

    /** @see org.geotools.xml.XMLElementHandler#getValue() */
    @Override
    public Object getValue() {
        return null;
    }

    /** @see org.geotools.xml.XMLElementHandler#getName() */
    @Override
    public String getName() {
        return NAME;
    }

    /** @see org.geotools.xml.XMLElementHandler#endElement(java.lang.String, java.lang.String) */
    @Override
    public void endElement(URI namespaceURI, String localName, Map<String, Object> hints) {
        // do nothing
    }

    /**
     * @see org.geotools.xml.XMLElementHandler#startElement(java.lang.String, java.lang.String, org.xml.sax.Attributes)
     */
    @Override
    public void startElement(URI namespaceURI, String localName, Attributes attr) {
        // do nothing
    }
}
