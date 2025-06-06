/*
 *    GeoTools - The Open Source Java GIS Toolkit
 *    http://geotools.org
 *
 *    (C) 2002-2022, Open Source Geospatial Foundation (OSGeo)
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
package org.geotools.data.informix;

import org.geotools.jdbc.JDBCPrimaryKeyTestSetup;

public class InformixPrimaryKeyTestSetup extends JDBCPrimaryKeyTestSetup {

    protected InformixPrimaryKeyTestSetup() {
        super(new InformixTestSetup());
    }

    @Override
    protected void createAutoGeneratedPrimaryKeyTable() throws Exception {
        run("CREATE TABLE auto ( pkey SERIAL PRIMARY KEY, " + "name VARCHAR(255), geom ST_POINT)");

        run("INSERT INTO auto (name,geom ) VALUES ('one',NULL)");
        run("INSERT INTO auto (name,geom ) VALUES ('two',NULL)");
        run("INSERT INTO auto (name,geom ) VALUES ('three',NULL)");
    }

    @Override
    protected void createSequencedPrimaryKeyTable() throws Exception {
        run("CREATE TABLE seq (key INT PRIMARY KEY, name VARCHAR(255)," + " geom ST_GEOMETRY)");
        run("CREATE SEQUENCE seq_key_sequence START WITH 1");

        run("INSERT INTO seq VALUES (seq_key_sequence.NEXTVAL,'one',NULL)");
        run("INSERT INTO seq VALUES (seq_key_sequence.NEXTVAL,'two',NULL)");
        run("INSERT INTO seq VALUES (seq_key_sequence.NEXTVAL,'three',NULL)");
    }

    @Override
    protected void createNonIncrementingPrimaryKeyTable() throws Exception {
        run("CREATE TABLE noninc ( pkey int PRIMARY KEY, name VARCHAR(255), geom ST_GEOMETRY)");
        run("INSERT INTO noninc VALUES (1, 'one', NULL)");
        run("INSERT INTO noninc VALUES (2, 'two', NULL)");
        run("INSERT INTO noninc VALUES (3, 'three', NULL)");
    }

    @Override
    protected void createMultiColumnPrimaryKeyTable() throws Exception {
        run("CREATE TABLE multi ( pkey1 int NOT NULL, pkey2 VARCHAR(255) NOT NULL, "
                + "name VARCHAR(255), geom ST_GEOMETRY)");
        run("ALTER TABLE multi ADD CONSTRAINT PRIMARY KEY (pkey1,pkey2)");

        run("INSERT INTO multi VALUES (1, 'x', 'one', NULL)");
        run("INSERT INTO multi VALUES (2, 'y', 'two', NULL)");
        run("INSERT INTO multi VALUES (3, 'z', 'three', NULL)");
    }

    @Override
    protected void createNullPrimaryKeyTable() throws Exception {
        run("CREATE TABLE nokey ( name VARCHAR(255) )");

        run("INSERT INTO nokey VALUES ('one')");
        run("INSERT INTO nokey VALUES ('two')");
        run("INSERT INTO nokey VALUES ('three')");
    }

    @Override
    protected void createUniqueIndexTable() throws Exception {
        run("CREATE TABLE uniq (pkey int, name VARCHAR(255), geom ST_POINT)");
        run("CREATE UNIQUE INDEX uniq_key_index ON uniq(pkey)");

        run("INSERT INTO uniq VALUES (1,'one',NULL)");
        run("INSERT INTO uniq VALUES (2,'two',NULL)");
        run("INSERT INTO uniq VALUES (3,'three',NULL)");
    }

    @Override
    protected void createNonFirstColumnPrimaryKey() throws Exception {
        run("CREATE TABLE nonfirst (name VARCHAR(255), pkey SERIAL PRIMARY KEY, geom ST_POINT)");

        run("INSERT INTO nonfirst (name,geom ) VALUES ('one',NULL)");
        run("INSERT INTO nonfirst (name,geom ) VALUES ('two',NULL)");
        run("INSERT INTO nonfirst (name,geom ) VALUES ('three',NULL)");
    }

    @Override
    protected void dropAutoGeneratedPrimaryKeyTable() throws Exception {
        run("DROP TABLE auto");
    }

    @Override
    protected void dropSequencedPrimaryKeyTable() throws Exception {
        run("DROP TABLE seq");
        run("DROP SEQUENCE seq_key_sequence");
    }

    @Override
    protected void dropNonIncrementingPrimaryKeyTable() throws Exception {
        run("DROP TABLE noninc");
    }

    @Override
    protected void dropMultiColumnPrimaryKeyTable() throws Exception {
        run("DROP TABLE multi");
    }

    @Override
    protected void dropNullPrimaryKeyTable() throws Exception {
        run("DROP TABLE nokey");
    }

    @Override
    protected void dropUniqueIndexTable() throws Exception {
        run("DROP TABLE uniq");
    }

    @Override
    protected void dropNonFirstPrimaryKeyTable() throws Exception {
        run("DROP TABLE nonfirst");
    }
}
