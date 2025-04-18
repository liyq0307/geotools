/*
 *    GeoTools - The Open Source Java GIS Toolkit
 *    http://geotools.org
 *
 *    (C) 2009-2011, Open Source Geospatial Foundation (OSGeo)
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
package org.geotools.appschema.filter.expression;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.geotools.api.filter.capability.FunctionName;
import org.geotools.api.filter.expression.Expression;
import org.geotools.api.filter.expression.ExpressionVisitor;
import org.geotools.api.filter.expression.Function;
import org.geotools.api.filter.expression.Literal;
import org.geotools.filter.capability.FunctionNameImpl;
import org.geotools.util.Converters;
import org.geotools.util.SoftValueHashMap;

/**
 * Vocabulary translation; using an external lookup table.
 *
 * <p>This is similar to a Recode function from the Symbology Enoding 1.1 specifcation with the difference that the
 * lookup table is named by a URI. This URI can be handled internally as an optimization; or it can be resolved to an
 * external URL which is dragged down (as a property file) and cached.
 *
 * <p>This function expects:
 *
 * <ol>
 *   <li>Expression: often a property name expression
 *   <li>Literal: URI defining the lookup table to use
 * </ol>
 *
 * @author Jody Garnett (GeoServer)
 */
public class VocabFunction implements Function {

    private final List<Expression> parameters;

    private final Literal fallback;

    public static final FunctionName NAME = new FunctionNameImpl("Vocab", "expr", "vocab");

    public VocabFunction() {
        this(new ArrayList<>(), null);
    }

    public VocabFunction(List<Expression> parameters, Literal fallback) {
        this.parameters = parameters;
        this.fallback = fallback;
    }

    @Override
    public String getName() {
        return NAME.getName();
    }

    @Override
    public FunctionName getFunctionName() {
        return NAME;
    }

    @Override
    public List<Expression> getParameters() {
        return Collections.unmodifiableList(parameters);
    }

    @Override
    public Object accept(ExpressionVisitor visitor, Object extraData) {
        return visitor.visit(this, extraData);
    }

    @Override
    public Object evaluate(Object object) {
        return evaluate(object, Object.class);
    }

    @Override
    public <T> T evaluate(Object object, Class<T> context) {
        final Expression expr = parameters.get(0);
        Expression vocab = parameters.get(1);

        String key = expr.evaluate(object, String.class);
        String urn = vocab.evaluate(object, String.class);

        Properties lookup = lookup(urn);
        if (lookup == null) {
            throw new RuntimeException("Unable to resolve lookup table " + urn);
        }
        return Converters.convert(lookup.get(key), context);
    }

    static Map<String, Properties> cache = Collections.synchronizedMap(new SoftValueHashMap<>());

    public static synchronized Properties lookup(String urn) {
        // We should look up in our Registery
        // (or in a perfrect world JNDI directory)
        // for this stuff
        Properties properties;

        if (cache.containsKey(urn)) {
            properties = cache.get(urn);
            if (properties == null) {
                throw new RuntimeException("Could not find file for lookup table " + urn);
            }
            return properties;
        }
        properties = new Properties();
        File file = new File(urn);
        if (file.exists()) {
            try (InputStream input = new BufferedInputStream(new FileInputStream(file))) {
                properties.load(input);
            } catch (FileNotFoundException e) {
                throw new RuntimeException("Could not find file for lookup table " + urn);
            } catch (IOException e) {
                throw new RuntimeException("Difficulty parsing lookup table " + urn);
            }
        } else {
            cache.put(urn, null); // don't check again and waste our time
            return null;
        }
        return properties;
    }

    @Override
    public Literal getFallbackValue() {
        return fallback;
    }
}
