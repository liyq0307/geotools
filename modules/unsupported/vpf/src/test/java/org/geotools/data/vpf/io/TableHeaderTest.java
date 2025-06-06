/*
 *    GeoTools - The Open Source Java GIS Toolkit
 *    http://geotools.org
 *
 *    (C) 2003-2008, Open Source Geospatial Foundation (OSGeo)
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
/*
 * File is generated by 'Unit Tests Generator' developed under
 * 'Web Test Tools' project at http://sf.net/projects/wttools/
 * Copyright (C) 2001 "Artur Hefczyc" <kobit@users.sourceforge.net>
 * to all 'Web Test Tools' subprojects.
 *
 * No rights to files and no responsibility for code generated
 * by this tool are belonged to author of 'unittestsgen' utility.
 *
 */
package org.geotools.data.vpf.io;

import java.io.File;
import java.io.IOException;
import org.geotools.data.vpf.ifc.DataTypesDefinition;
import org.geotools.data.vpf.ifc.VPFHeader;
import org.geotools.test.TestData;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

// TODO TableInputStream was deprecated in 2.0.x and has been removed
// in 2.2.x. This file needs to be updated and tests re-enabled.

/**
 * File <code>TableHeaderTest.java</code> is automaticaly generated by 'unittestsgen' application. Code generator is
 * created for java sources and for 'junit' package by "Artur Hefczyc" <kobit@users.sourceforge.net><br>
 * You should fulfil test methods with proper code for testing purpose. All methods where you should put your code are
 * below and their names starts with 'test'.<br>
 * You can run unit tests in many ways, however prefered are:
 *
 * <ul>
 *   <li>Run tests for one class only, for example for this class you can run tests with command:
 *       <pre>
 *       java -cp "jar/thisjarfile.jar;lib/junit.jar" org.geotools.vpf.TableHeaderTest
 * </pre>
 *   <li>Run tests for all classes in one command call. Code generator creates also <code>
 *       TestAll.class</code> which runs all available tests:
 *       <pre>
 *       java -cp "jar/thisjarfile.jar;lib/junit.jar" TestAll
 * </pre>
 *   <li>But the most prefered way is to run all tests from <em>Ant</em> just after compilation process finished.<br>
 *       To do it. You need:
 *       <ol>
 *         <li>Ant package from <a href="http://jakarta.apache.org/">Ant</a>
 *         <li>JUnit package from <a href="http://www.junit.org/">JUnit</a>
 *         <li>Put some code in your <code>build.xml</code> file to tell Ant how to test your package. Sample code for
 *             Ant's <code>build.xml</code> you can find in created file: <code>sample-junit-build.xml</code>. And
 *             remember to have <code>junit.jar</code> in CLASSPATH <b>before</b> you run Ant. To generate reports by
 *             ant you must have <code>
 *             xalan.jar</code> in your <code>ANT_HOME/lib/</code> directory.
 *       </ol>
 * </ul>
 *
 * @source $URL$
 */
public class TableHeaderTest {
    /** Instance of tested class. */
    protected VPFHeader varTableHeader;

    /**
     * This method is called every time before particular test execution. It creates new instance of tested class and it
     * can perform some more actions which are necessary for performs tests.
     */
    @Before
    public void setUp() throws IOException {
        File dht = TestData.file(this, "dnc13/dht");
        VPFInputStream tis = new VariableIndexInputStream(dht.getPath(), DataTypesDefinition.LITTLE_ENDIAN_ORDER);
        varTableHeader = tis.getHeader();
        tis.close();
    } // end of setUp()

    /** Method for testing original source method: char getByteOrder() from tested class */
    @Test
    public void testGetByteOrder() {
        // assertEquals("Checking byte order detection.", 'L', varTableHeader.getByteOrder());
    } // end of testGetByteOrder()

    /** Method for testing original source method: java.util.List getColumnDefs() from tested class */
    @Test
    @Ignore
    public void testGetColumnDefs() {
        //        assertEquals("Checking number of detected column definitions.", 20, varTableHeader
        //                .getColumnDefs().size());
    } // end of testGetColumnDefs()

    /** Method for testing original source method: java.lang.String getDescription() from tested class */
    @Test
    @Ignore
    public void testGetDescription() {
        //        assertEquals("Cheking description detection.", "Database Header Table",
        //                varTableHeader.getDescription());
    } // end of testGetDescription()

    /** Method for testing original source method: int getLength() from tested class */
    @Test
    @Ignore
    public void testGetLength() {
        Assert.assertEquals("Cheking header length detection.", 1261, varTableHeader.getLength());
    } // end of testGetLength()

    /** Method for testing original source method: java.lang.String getNarrativeTable() from tested class */
    @Test
    @Ignore
    public void testGetNarrativeTable() {
        //        assertNull("Cheking narrative table name detection.",
        // varTableHeader.getNarrativeTable());
    } // end of testGetNarrativeTable()
} // end of TableHeaderTest
