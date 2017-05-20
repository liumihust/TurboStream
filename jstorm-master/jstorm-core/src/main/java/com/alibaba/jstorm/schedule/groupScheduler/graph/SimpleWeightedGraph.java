/* ==========================================
 * JGraphT : a free Java graph-theory library
 * ==========================================
 *
 * Project Info:  http://jgrapht.sourceforge.net/
 * Project Creator:  Barak Naveh (http://sourceforge.net/users/barak_naveh)
 *
 * (C) Copyright 2003-2007, by Barak Naveh and Contributors.
 *
 * This library is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation; either version 2.1 of the License, or
 * (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public
 * License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, write to the Free Software Foundation,
 * Inc.,
 * 59 Temple Place, Suite 330, Boston, MA 02111-1307, USA.
 */
/* ------------------------
 * SimpleWeightedGraph.java
 * ------------------------
 * (C) Copyright 2003-2007, by Barak Naveh and Contributors.
 *
 * Original Author:  Barak Naveh
 * Contributor(s):   Christian Hammer
 *
 * $Id: SimpleWeightedGraph.java 568 2007-09-30 00:12:18Z perfecthash $
 *
 * Changes
 * -------
 * 05-Aug-2003 : Initial revision (BN);
 * 06-Aug-2005 : Made generic (CH);
 * 28-May-2006 : Moved connectivity info from edge to graph (JVS);
 *
 */
package com.alibaba.jstorm.schedule.groupScheduler.graph;


import java.util.Collection;
import java.util.Set;

/**
 * A simple weighted graph. A simple weighted graph is a simple graph for which
 * edges are assigned weights.
 */
public class SimpleWeightedGraph<V, E>
    extends AbstractBaseGraph<V, E>
    implements WeightedGraph<V, E>,UndirectedGraph<V, E>
{
    //~ Static fields/initializers ---------------------------------------------

    private static final long serialVersionUID = 3906088949100655922L;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new simple weighted graph with the specified edge factory.
     *
     * @param ef the edge factory of the new graph.
     */
    public SimpleWeightedGraph(EdgeFactory<V, E> ef)
    {
    	super(ef, false, false);
    }

    /**
     * Creates a new simple weighted graph.
     *
     * @param edgeClass class on which to base factory for edges
     */
    public SimpleWeightedGraph(Class<? extends E> edgeClass)
    {
        this(new ClassBasedEdgeFactory<V, E>(edgeClass));
    }

    public boolean containsEdge(V sourceVertex, V targetVertex){
        return getEdge(sourceVertex, targetVertex) != null;
    }

    public boolean removeAllEdges(Collection<? extends E> edges){
        boolean modified = false;

        for (E e : edges) {
            modified |= removeEdge(e);
        }
        return modified;
    }

    public  boolean removeAllEdges(E [] edges)
    {
        boolean modified = false;
        for (int i = 0; i < edges.length; i++) {
            modified |= removeEdge(edges[i]);
        }

        return modified;
    }
    public Set<E> removeAllEdges(V sourceVertex, V targetVertex){
        Set<E> removed = getAllEdges(sourceVertex, targetVertex);
        removeAllEdges(removed);

        return removed;
    }

    public boolean removeAllVertices(Collection<? extends V> vertices){
        boolean modified = false;

        for (V v : vertices) {
            modified |= removeVertex(v);
        }

        return modified;
    }
    public boolean assertVertexExist(V v)
    {
        if (containsVertex(v)) {
            return true;
        } else if (v == null) {
            throw new NullPointerException();
        } else {
            throw new IllegalArgumentException("no such vertex in graph");
        }
    }



}

// End SimpleWeightedGraph.java
