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
package org.geotools.graph.structure.opt;

import java.util.Iterator;
import org.geotools.graph.structure.Edge;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class OptEdgeTest {

    private OptNode m_nodeA;
    private OptNode m_nodeB;
    private OptNode m_otherNode1;
    private OptNode m_otherNode2;
    private OptNode m_otherNode3;
    private OptNode m_otherNode4;

    private OptEdge m_edge;
    private OptEdge m_other1;
    private OptEdge m_other2;
    private OptEdge m_other3;
    private OptEdge m_other4;

    private OptEdge m_same;
    private OptEdge m_opp;
    private OptEdge m_loopA;
    private OptEdge m_loopB;

    @Before
    public void setUp() throws Exception {
        m_nodeA = new OptNode();
        m_nodeA.setDegree(3);
        m_nodeB = new OptNode();
        m_nodeB.setDegree(3);
        m_otherNode1 = new OptNode();
        m_otherNode1.setDegree(1);
        m_otherNode2 = new OptNode();
        m_otherNode2.setDegree(1);
        m_otherNode3 = new OptNode();
        m_otherNode3.setDegree(1);
        m_otherNode4 = new OptNode();
        m_otherNode4.setDegree(1);

        m_edge = new OptEdge(m_nodeA, m_nodeB);
        m_nodeA.add(m_edge);
        m_nodeB.add(m_edge);

        m_other1 = new OptEdge(m_nodeA, m_otherNode1);
        m_nodeA.add(m_other1);
        m_otherNode1.add(m_other1);

        m_other2 = new OptEdge(m_nodeB, m_otherNode2);
        m_nodeB.add(m_other2);
        m_otherNode2.add(m_other2);

        m_other3 = new OptEdge(m_otherNode3, m_nodeA);
        m_otherNode3.add(m_other3);
        m_nodeA.add(m_other3);

        m_other4 = new OptEdge(m_otherNode4, m_nodeB);
        m_otherNode4.add(m_other4);
        m_nodeB.add(m_other4);

        // dont add these to the graph yet
        m_same = new OptEdge(m_nodeA, m_nodeB);
        m_opp = new OptEdge(m_nodeB, m_nodeA);
        m_loopA = new OptEdge(m_nodeA, m_nodeA);
        m_loopB = new OptEdge(m_nodeB, m_nodeB);
    }

    protected void addSame() {
        try {
            setUp();
        } catch (Exception e) {
            java.util.logging.Logger.getGlobal().log(java.util.logging.Level.INFO, "", e);
        }

        m_nodeA.setDegree(4);
        m_nodeB.setDegree(4);

        m_nodeA.add(m_edge);
        m_nodeA.add(m_other1);
        m_nodeA.add(m_other3);
        m_nodeA.add(m_same);

        m_nodeB.add(m_edge);
        m_nodeB.add(m_other2);
        m_nodeB.add(m_other4);
        m_nodeB.add(m_same);
    }

    protected void addOpp() {
        try {
            setUp();
        } catch (Exception e) {
            java.util.logging.Logger.getGlobal().log(java.util.logging.Level.INFO, "", e);
        }

        m_nodeA.setDegree(4);
        m_nodeB.setDegree(4);

        m_nodeA.add(m_edge);
        m_nodeA.add(m_other1);
        m_nodeA.add(m_other3);
        m_nodeA.add(m_opp);

        m_nodeB.add(m_edge);
        m_nodeB.add(m_other2);
        m_nodeB.add(m_other4);
        m_nodeB.add(m_opp);
    }

    protected void addLoopA() {
        try {
            setUp();
        } catch (Exception e) {
            java.util.logging.Logger.getGlobal().log(java.util.logging.Level.INFO, "", e);
        }

        m_nodeA.setDegree(4);

        m_nodeA.add(m_edge);
        m_nodeA.add(m_other1);
        m_nodeA.add(m_other3);
        m_nodeA.add(m_loopA);
    }

    protected void addLoopB() {
        try {
            setUp();
        } catch (Exception e) {
            java.util.logging.Logger.getGlobal().log(java.util.logging.Level.INFO, "", e);
        }

        m_nodeB.setDegree(4);

        m_nodeB.add(m_edge);
        m_nodeB.add(m_other2);
        m_nodeB.add(m_other4);
        m_nodeB.add(m_loopB);
    }

    protected void addLoopAB() {
        try {
            setUp();
        } catch (Exception e) {
            java.util.logging.Logger.getGlobal().log(java.util.logging.Level.INFO, "", e);
        }

        m_nodeA.setDegree(4);

        m_nodeA.add(m_edge);
        m_nodeA.add(m_other1);
        m_nodeA.add(m_other3);
        m_nodeA.add(m_loopA);

        m_nodeB.setDegree(4);

        m_nodeB.add(m_edge);
        m_nodeB.add(m_other2);
        m_nodeB.add(m_other4);
        m_nodeB.add(m_loopB);
    }

    @Test
    public void test_getNodeA() {
        Assert.assertSame(m_edge.getNodeA(), m_nodeA);
    }

    @Test
    public void test_getNodeB() {
        Assert.assertSame(m_edge.getNodeB(), m_nodeB);
    }

    @Test
    public void test_getOtherNode() {
        Assert.assertSame(m_edge.getOtherNode(m_nodeA), m_nodeB);
        Assert.assertSame(m_edge.getOtherNode(m_nodeB), m_nodeA);
        Assert.assertSame(m_edge.getOtherNode(new OptNode()), null);
    }

    @Test
    public void test_reverse() {
        Assert.assertSame(m_nodeA, m_edge.getNodeA());
        Assert.assertSame(m_nodeB, m_edge.getNodeB());

        m_edge.reverse();

        Assert.assertSame(m_nodeA, m_edge.getNodeB());
        Assert.assertSame(m_nodeB, m_edge.getNodeA());
    }

    @Test
    public void test_getRelated() {

        // nodes share single edge
        Iterator itr = m_edge.getRelated();
        Assert.assertTrue(itr.hasNext());

        OptEdge be = (OptEdge) itr.next();
        Assert.assertTrue(be.equals(m_other1) || be.equals(m_other2) || be.equals(m_other3) || be.equals(m_other4));
        Assert.assertTrue(itr.hasNext());

        be = (OptEdge) itr.next();
        Assert.assertTrue(be.equals(m_other1) || be.equals(m_other2) || be.equals(m_other3) || be.equals(m_other4));
        Assert.assertTrue(itr.hasNext());
        be = (OptEdge) itr.next();
        Assert.assertTrue(be.equals(m_other1) || be.equals(m_other2) || be.equals(m_other3) || be.equals(m_other4));
        Assert.assertTrue(itr.hasNext());

        be = (OptEdge) itr.next();
        Assert.assertTrue(be.equals(m_other1) || be.equals(m_other2) || be.equals(m_other3) || be.equals(m_other4));
        Assert.assertFalse(itr.hasNext());

        // nodes share multiple edges (same direction)
        addSame();

        itr = m_edge.getRelated();
        Assert.assertTrue(itr.hasNext());

        be = (OptEdge) itr.next();
        Assert.assertTrue(be.equals(m_other1)
                || be.equals(m_other2)
                || be.equals(m_other3)
                || be.equals(m_other4)
                || be.equals(m_same));
        Assert.assertTrue(itr.hasNext());

        be = (OptEdge) itr.next();
        Assert.assertTrue(be.equals(m_other1)
                || be.equals(m_other2)
                || be.equals(m_other3)
                || be.equals(m_other4)
                || be.equals(m_same));
        Assert.assertTrue(itr.hasNext());

        be = (OptEdge) itr.next();
        Assert.assertTrue(be.equals(m_other1)
                || be.equals(m_other2)
                || be.equals(m_other3)
                || be.equals(m_other4)
                || be.equals(m_same));
        Assert.assertTrue(itr.hasNext());

        be = (OptEdge) itr.next();
        Assert.assertTrue(be.equals(m_other1)
                || be.equals(m_other2)
                || be.equals(m_other3)
                || be.equals(m_other4)
                || be.equals(m_same));
        Assert.assertTrue(itr.hasNext());

        be = (OptEdge) itr.next();
        Assert.assertTrue(be.equals(m_other1)
                || be.equals(m_other2)
                || be.equals(m_other3)
                || be.equals(m_other4)
                || be.equals(m_same));
        Assert.assertFalse(itr.hasNext());

        // nodes share multiple edges (differnt direction)
        addOpp();

        itr = m_edge.getRelated();
        Assert.assertTrue(itr.hasNext());

        be = (OptEdge) itr.next();
        Assert.assertTrue(be.equals(m_other1)
                || be.equals(m_other2)
                || be.equals(m_other3)
                || be.equals(m_other4)
                || be.equals(m_opp));
        Assert.assertTrue(itr.hasNext());

        be = (OptEdge) itr.next();
        Assert.assertTrue(be.equals(m_other1)
                || be.equals(m_other2)
                || be.equals(m_other3)
                || be.equals(m_other4)
                || be.equals(m_opp));
        Assert.assertTrue(itr.hasNext());

        be = (OptEdge) itr.next();
        Assert.assertTrue(be.equals(m_other1)
                || be.equals(m_other2)
                || be.equals(m_other3)
                || be.equals(m_other4)
                || be.equals(m_opp));
        Assert.assertTrue(itr.hasNext());

        be = (OptEdge) itr.next();
        Assert.assertTrue(be.equals(m_other1)
                || be.equals(m_other2)
                || be.equals(m_other3)
                || be.equals(m_other4)
                || be.equals(m_opp));
        Assert.assertTrue(itr.hasNext());

        be = (OptEdge) itr.next();
        Assert.assertTrue(be.equals(m_other1)
                || be.equals(m_other2)
                || be.equals(m_other3)
                || be.equals(m_other4)
                || be.equals(m_opp));
        Assert.assertFalse(itr.hasNext());

        // loop on one of nodes
        addLoopA();

        itr = m_edge.getRelated();
        Assert.assertTrue(itr.hasNext());

        be = (OptEdge) itr.next();
        Assert.assertTrue(be.equals(m_other1)
                || be.equals(m_other2)
                || be.equals(m_other3)
                || be.equals(m_other4)
                || be.equals(m_loopA));
        Assert.assertTrue(itr.hasNext());

        be = (OptEdge) itr.next();
        Assert.assertTrue(be.equals(m_other1)
                || be.equals(m_other2)
                || be.equals(m_other3)
                || be.equals(m_other4)
                || be.equals(m_loopA));

        be = (OptEdge) itr.next();
        Assert.assertTrue(be.equals(m_other1)
                || be.equals(m_other2)
                || be.equals(m_other3)
                || be.equals(m_other4)
                || be.equals(m_loopA));

        be = (OptEdge) itr.next();
        Assert.assertTrue(be.equals(m_other1)
                || be.equals(m_other2)
                || be.equals(m_other3)
                || be.equals(m_other4)
                || be.equals(m_loopA));
        Assert.assertTrue(itr.hasNext());

        be = (OptEdge) itr.next();
        Assert.assertTrue(be.equals(m_other1)
                || be.equals(m_other2)
                || be.equals(m_other3)
                || be.equals(m_other4)
                || be.equals(m_loopA));
        Assert.assertFalse(itr.hasNext());

        // loop on other node
        addLoopB();

        itr = m_edge.getRelated();
        Assert.assertTrue(itr.hasNext());

        be = (OptEdge) itr.next();
        Assert.assertTrue(be.equals(m_other1)
                || be.equals(m_other2)
                || be.equals(m_other3)
                || be.equals(m_other4)
                || be.equals(m_loopB));
        Assert.assertTrue(itr.hasNext());

        be = (OptEdge) itr.next();
        Assert.assertTrue(be.equals(m_other1)
                || be.equals(m_other2)
                || be.equals(m_other3)
                || be.equals(m_other4)
                || be.equals(m_loopB));

        be = (OptEdge) itr.next();
        Assert.assertTrue(be.equals(m_other1)
                || be.equals(m_other2)
                || be.equals(m_other3)
                || be.equals(m_other4)
                || be.equals(m_loopB));

        be = (OptEdge) itr.next();
        Assert.assertTrue(be.equals(m_other1)
                || be.equals(m_other2)
                || be.equals(m_other3)
                || be.equals(m_other4)
                || be.equals(m_loopB));
        Assert.assertTrue(itr.hasNext());

        be = (OptEdge) itr.next();
        Assert.assertTrue(be.equals(m_other1)
                || be.equals(m_other2)
                || be.equals(m_other3)
                || be.equals(m_other4)
                || be.equals(m_loopB));
        Assert.assertFalse(itr.hasNext());

        // loop on both
        addLoopAB();

        itr = m_edge.getRelated();
        Assert.assertTrue(itr.hasNext());

        be = (OptEdge) itr.next();
        Assert.assertTrue(be.equals(m_other1)
                || be.equals(m_other2)
                || be.equals(m_loopA)
                || be.equals(m_other3)
                || be.equals(m_other4)
                || be.equals(m_loopB));
        Assert.assertTrue(itr.hasNext());

        be = (OptEdge) itr.next();
        Assert.assertTrue(be.equals(m_other1)
                || be.equals(m_other2)
                || be.equals(m_loopA)
                || be.equals(m_other3)
                || be.equals(m_other4)
                || be.equals(m_loopB));

        be = (OptEdge) itr.next();
        Assert.assertTrue(be.equals(m_other1)
                || be.equals(m_other2)
                || be.equals(m_loopA)
                || be.equals(m_other3)
                || be.equals(m_other4)
                || be.equals(m_loopB));

        be = (OptEdge) itr.next();
        Assert.assertTrue(be.equals(m_other1)
                || be.equals(m_other2)
                || be.equals(m_loopA)
                || be.equals(m_other3)
                || be.equals(m_other4)
                || be.equals(m_loopA));
        Assert.assertTrue(itr.hasNext());

        be = (OptEdge) itr.next();
        Assert.assertTrue(be.equals(m_other1)
                || be.equals(m_other2)
                || be.equals(m_loopA)
                || be.equals(m_other3)
                || be.equals(m_other4)
                || be.equals(m_loopB));

        be = (OptEdge) itr.next();
        Assert.assertTrue(be.equals(m_other1)
                || be.equals(m_other2)
                || be.equals(m_loopA)
                || be.equals(m_other3)
                || be.equals(m_other4)
                || be.equals(m_loopB));

        Assert.assertFalse(itr.hasNext());
    }

    @Test
    public void test_compareTo() {
        OptEdge same = new OptEdge(m_nodeA, m_nodeB);
        OptEdge opp = new OptEdge(m_nodeB, m_nodeA);

        Assert.assertEquals(m_edge.compareNodes(same), Edge.EQUAL_NODE_ORIENTATION);
        Assert.assertEquals(m_edge.compareNodes(opp), Edge.OPPOSITE_NODE_ORIENTATION);
        Assert.assertEquals(m_edge.compareNodes(m_other1), Edge.UNEQUAL_NODE_ORIENTATION);
    }
}
