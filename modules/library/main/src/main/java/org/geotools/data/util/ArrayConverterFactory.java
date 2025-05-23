/*
 *    GeoTools - The Open Source Java GIS Toolkit
 *    http://geotools.org
 *
 *    (C) 2002-2008, Open Source Geospatial Foundation (OSGeo)
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
package org.geotools.data.util;

import java.lang.reflect.Array;
import org.geotools.util.Converter;
import org.geotools.util.ConverterFactory;
import org.geotools.util.Converters;
import org.geotools.util.factory.Hints;

/** Converter factory converting objects to single element arrays and vice-versa */
public class ArrayConverterFactory implements ConverterFactory {
    @Override
    public Converter createConverter(Class<?> source, Class<?> target, Hints hints) {
        if (source.isArray() && !target.isArray() && source.getComponentType().isAssignableFrom(target)) {
            return new ArrayToSingleConverter();
        } else if (target.isArray()
                && !source.isArray()
                && target.getComponentType().isAssignableFrom(source)) {
            return new SingleToArrayConverter();
        } else if (source.isArray() && target.isArray()) {
            return new ArrayToArrayConverter();
        }
        return null;
    }

    private static class ArrayToSingleConverter implements Converter {
        @Override
        public <T> T convert(Object source, Class<T> target) throws Exception {
            if (Array.getLength(source) != 1) {
                return null;
            }
            return target.cast(Array.get(source, 0));
        }
    }

    private static class SingleToArrayConverter implements Converter {
        @Override
        public <T> T convert(Object source, Class<T> target) throws Exception {
            Object result = Array.newInstance(target.getComponentType(), 1);
            Array.set(result, 0, source);
            return target.cast(result);
        }
    }

    private static class ArrayToArrayConverter implements Converter {
        @Override
        public <T> T convert(Object source, Class<T> target) throws Exception {
            int arrLength = Array.getLength(source);
            Class<?> elementType = target.getComponentType();
            Object result = Array.newInstance(elementType, arrLength);

            for (int count = 0; count < arrLength; count++) {
                Object sourceElement = Array.get(source, count);
                Object converted = Converters.convert(sourceElement, elementType);
                if (sourceElement != null && converted == null) {
                    // error in conversion
                    return null;
                }
                Array.set(result, count, converted);
            }
            return target.cast(result);
        }
    }
}
