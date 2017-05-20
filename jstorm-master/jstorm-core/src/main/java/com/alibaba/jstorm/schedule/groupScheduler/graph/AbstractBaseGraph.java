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
/* ----------------------
 * AbstractBaseGraph.java
 * ----------------------
 * (C) Copyright 2003-2007, by Barak Naveh and Contributors.
 *
 * Original Author:  Barak Naveh
 * Contributor(s):   John V. Sichi
 *                   Christian Hammer
 *
 * $Id: AbstractBaseGraph.java 568 2007-09-30 00:12:18Z perfecthash $
 *
 * Changes
 * -------
 * 24-Jul-2003 : Initial revision (BN);
 * 10-Aug-2003 : General edge refactoring (BN);
 * 06-Nov-2003 : Change edge sharing semantics (JVS);
 * 07-Feb-2004 : Enabled serialization (BN);
 * 11-Mar-2004 : Made generic (CH);
 * 01-Jun-2005 : Added EdgeListFactory (JVS);
 * 07-May-2006 : Changed from List<Edge> to Set<Edge> (JVS);
 * 28-May-2006 : Moved connectivity info from edge to graph (JVS);
 *
 */
package com.alibaba.jstorm.schedule.groupScheduler.graph;

import java.io.*;
import java.util.*;



/**
 * The most general implementation of the {@link org.jgrapht.Graph} interface.
 * Its subclasses add various restrictions to get more specific graphs. The
 * decision whether it is directed or undirected is decided at construction time
 * and cannot be later modified (see constructor for details).
 *
 * <p>This graph implementation guarantees deterministic vertex and edge set
 * ordering (via {@link LinkedHashMap} and {@link LinkedHashSet}).</p>
 *
 */
public abstract class AbstractBaseGraph<V, E> implements Graph<V, E>,
        Cloneable,
        Serializable
{
	private static final long serialVersionUID = 1L;

	private static final String LOOPS_NOT_ALLOWED = "loops not allowed";

    //~ Instance fields --------------------------------------------------------

    private EdgeFactory<V, E> edgeFactory;
    private EdgeSetFactory<V, E> edgeSetFactory;
    private Map<E, EdgeIntrusive> edgeMap;
    private transient Set<E> unmodifiableEdgeSet = null;
    private transient Set<V> unmodifiableVertexSet = null;
    private Specifics specifics;

    private transient TypeUtil<V> vertexTypeDecl = null;

    //~ Constructors -----------------------------------------------------------

    /**
     * Construct a new pseudograph. The pseudograph can either be directed or
     * undirected, depending on the specified edge factory.
     *
     * @param ef the edge factory of the new graph.
     * @param allowMultipleEdges whether to allow multiple edges or not.
     * @param allowLoops whether to allow edges that are self-loops or not.
     *
     * @throws NullPointerException if the specified edge factory is <code>
     * null</code>.
     */

    public AbstractBaseGraph(
        EdgeFactory<V, E> ef,
        boolean allowMultipleEdges,
        boolean allowLoops)
    {
        if (ef == null) {
            throw new NullPointerException();
        }

        edgeMap = new LinkedHashMap<E, EdgeIntrusive>();
        edgeFactory = ef;

        specifics = createSpecifics();

        this.edgeSetFactory = new ArrayListFactory<V, E>();
    }

    //~ Methods ----------------------------------------------------------------








    /**
     * @see Graph#getAllEdges(Object, Object)
     */
    public Set<E> getAllEdges(V sourceVertex, V targetVertex)
    {
        return specifics.getAllEdges(sourceVertex, targetVertex);
    }

    /**
     * @see Graph#getEdge(Object, Object)
     */
    public E getEdge(V sourceVertex, V targetVertex)
    {
        return specifics.getEdge(sourceVertex, targetVertex);
    }

    /**
     * @see Graph#getEdgeFactory()
     */
    public EdgeFactory<V, E> getEdgeFactory()
    {
        return edgeFactory;
    }

    /**
     * Set the {@link EdgeSetFactory} to use for this graph. Initially, a graph
     * is created with a default implementation which always supplies an {@link
     * ArrayList} with capacity 1.
     *
     * @param edgeSetFactory factory to use for subsequently created edge sets
     * (this call has no effect on existing edge sets)
     */
    public void setEdgeSetFactory(EdgeSetFactory<V, E> edgeSetFactory)
    {
        this.edgeSetFactory = edgeSetFactory;
    }

    /**
     * @see Graph#addEdge(Object, Object)
     */
    public E addEdge(V sourceVertex, V targetVertex)
    {
        assertVertexExist(sourceVertex);
        assertVertexExist(targetVertex);

        if (containsEdge(sourceVertex, targetVertex))
        {
            return null;
        }

        if (sourceVertex.equals(targetVertex)) {
            throw new IllegalArgumentException(LOOPS_NOT_ALLOWED);
        }

        E e = edgeFactory.createEdge(sourceVertex, targetVertex);

        if (containsEdge(e)) { // this restriction should stay!
            return null;
        } else {
            EdgeIntrusive intrusiveEdge =
                createIntrusiveEdge(e, sourceVertex, targetVertex);

            edgeMap.put(e, intrusiveEdge);
            specifics.addEdgeToTouchingVertices(e);

            return e;
        }
    }

    /**
     * @see Graph#addEdge(Object, Object, Object)
     */
    public boolean addEdge(V sourceVertex, V targetVertex, E e)
    {
        if (e == null) {
            throw new NullPointerException();
        } else if (containsEdge(e)) {
            return false;
        }

        assertVertexExist(sourceVertex);
        assertVertexExist(targetVertex);

        if (containsEdge(sourceVertex, targetVertex))
        {
            return false;
        }

        if (sourceVertex.equals(targetVertex)) {
            throw new IllegalArgumentException(LOOPS_NOT_ALLOWED);
        }

        EdgeIntrusive intrusiveEdge =
            createIntrusiveEdge(e, sourceVertex, targetVertex);

        edgeMap.put(e, intrusiveEdge);
        specifics.addEdgeToTouchingVertices(e);

        return true;
    }

    private EdgeIntrusive createIntrusiveEdge(
        E e,
        V sourceVertex,
        V targetVertex)
    {
        EdgeIntrusive intrusiveEdge;
        if (e instanceof EdgeIntrusive) {
            intrusiveEdge = (EdgeIntrusive) e;
        } else {
            intrusiveEdge = new EdgeIntrusive();
        }
        intrusiveEdge.source = sourceVertex;
        intrusiveEdge.target = targetVertex;
        return intrusiveEdge;
    }

    /**
     * @see Graph#addVertex(Object)
     */
    public boolean addVertex(V v)
    {
        if (v == null) {
            throw new NullPointerException();
        } else if (containsVertex(v)) {
            return false;
        } else {
            specifics.addVertex(v);

            return true;
        }
    }

    /**
     * @see Graph#getEdgeSource(Object)
     */
    public V getEdgeSource(E e)
    {
        return TypeUtil.uncheckedCast(
            getIntrusiveEdge(e).source,
            vertexTypeDecl);
    }

    /**
     * @see Graph#getEdgeTarget(Object)
     */
    public V getEdgeTarget(E e)
    {
        return TypeUtil.uncheckedCast(
            getIntrusiveEdge(e).target,
            vertexTypeDecl);
    }

    private EdgeIntrusive getIntrusiveEdge(E e)
    {
        if (e instanceof EdgeIntrusive) {
            return (EdgeIntrusive) e;
        }

        return edgeMap.get(e);
    }

    /**
     * Returns a shallow copy of this graph instance. Neither edges nor vertices
     * are cloned.
     *
     * @return a shallow copy of this set.
     *
     * @throws RuntimeException
     *
     * @see Object#clone()
     */
    public Object clone()
    {
        try {
            TypeUtil<AbstractBaseGraph<V, E>> typeDecl = null;

            AbstractBaseGraph<V, E> newGraph =
                TypeUtil.uncheckedCast(super.clone(), typeDecl);

            newGraph.edgeMap = new LinkedHashMap<E, EdgeIntrusive>();

            newGraph.edgeFactory = this.edgeFactory;
            newGraph.unmodifiableEdgeSet = null;
            newGraph.unmodifiableVertexSet = null;

            // NOTE:  it's important for this to happen in an object
            // method so that the new inner class instance gets associated with
            // the right outer class instance
            newGraph.specifics = newGraph.createSpecifics();

            //Graphs.addGraph(newGraph, this);

            return newGraph;
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
            throw new RuntimeException();
        }
    }

    /**
     * @see Graph#containsEdge(Object)
     */
    public boolean containsEdge(E e)
    {
        return edgeMap.containsKey(e);
    }

    /**
     * @see Graph#containsVertex(Object)
     */
    public boolean containsVertex(V v)
    {
        return specifics.getVertexSet().contains(v);
    }

    /**
     * @see UndirectedGraph#degreeOf(Object)
     */
    public int degreeOf(V vertex)
    {
        return specifics.degreeOf(vertex);
    }

    /**
     * @see Graph#edgeSet()
     */
    public Set<E> edgeSet()
    {
        if (unmodifiableEdgeSet == null) {
            unmodifiableEdgeSet = Collections.unmodifiableSet(edgeMap.keySet());
        }

        return unmodifiableEdgeSet;
    }

    /**
     * @see Graph#edgesOf(Object)
     */
    public Set<E> edgesOf(V vertex)
    {
        return specifics.edgesOf(vertex);
    }

    /**

     */
    public int inDegreeOf(V vertex)
    {
        return specifics.inDegreeOf(vertex);
    }

    /**
     *
     */
    public Set<E> incomingEdgesOf(V vertex)
    {
        return specifics.incomingEdgesOf(vertex);
    }

    /**
     *
     */
    public int outDegreeOf(V vertex)
    {
        return specifics.outDegreeOf(vertex);
    }

    /**
     *
     */
    public Set<E> outgoingEdgesOf(V vertex)
    {
        return specifics.outgoingEdgesOf(vertex);
    }

    /**
     * @see Graph#removeEdge(Object, Object)
     */
    public E removeEdge(V sourceVertex, V targetVertex)
    {
        E e = getEdge(sourceVertex, targetVertex);

        if (e != null) {
            specifics.removeEdgeFromTouchingVertices(e);
            edgeMap.remove(e);
        }

        return e;
    }

    /**
     * @see Graph#removeEdge(Object)
     */
    public boolean removeEdge(E e)
    {
        if (containsEdge(e)) {
            specifics.removeEdgeFromTouchingVertices(e);
            edgeMap.remove(e);

            return true;
        } else {
            return false;
        }
    }

    /**
     * @see Graph#removeVertex(Object)
     */
    public boolean removeVertex(V v)
    {
        if (containsVertex(v)) {
            Set<E> touchingEdgesList = edgesOf(v);

            // cannot iterate over list - will cause
            // ConcurrentModificationException
            removeAllEdges(new ArrayList<E>(touchingEdgesList));

            specifics.getVertexSet().remove(v); // remove the vertex itself

            return true;
        } else {
            return false;
        }
    }

    /**
     * @see Graph#vertexSet()
     */
    public Set<V> vertexSet()
    {
        if (unmodifiableVertexSet == null) {
            unmodifiableVertexSet =
                Collections.unmodifiableSet(specifics.getVertexSet());
        }

        return unmodifiableVertexSet;
    }

    /**
     * @see Graph#getEdgeWeight(Object)
     */
    public double getEdgeWeight(E e)
    {
        if (e instanceof EdgeWeighted) {
            return ((EdgeWeighted) e).weight;
        } else {
            return WeightedGraph.DEFAULT_EDGE_WEIGHT;
        }
    }

    /**
     * @see WeightedGraph#setEdgeWeight(Object, double)
     */
    public void setEdgeWeight(E e, double weight)
    {
        assert (e instanceof EdgeWeighted) : e.getClass();
        ((EdgeWeighted) e).weight = weight;
    }

    private Specifics createSpecifics()
    {
        if (this instanceof UndirectedGraph) {
            return new UndirectedSpecifics();
        } else {
            throw new IllegalArgumentException(
                "must be instance of either DirectedGraph or UndirectedGraph");
        }
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * .
     *
     * @author Barak Naveh
     */
    private abstract class Specifics
        implements Serializable
    {
        public abstract void addVertex(V vertex);

        public abstract Set<V> getVertexSet();

        /**
         * .
         *
         * @param sourceVertex
         * @param targetVertex
         *
         * @return
         */
        public abstract Set<E> getAllEdges(V sourceVertex,
            V targetVertex);

        /**
         * .
         *
         * @param sourceVertex
         * @param targetVertex
         *
         * @return
         */
        public abstract E getEdge(V sourceVertex, V targetVertex);

        /**
         * Adds the specified edge to the edge containers of its source and
         * target vertices.
         *
         * @param e
         */
        public abstract void addEdgeToTouchingVertices(E e);

        /**
         * .
         *
         * @param vertex
         *
         * @return
         */
        public abstract int degreeOf(V vertex);

        /**
         * .
         *
         * @param vertex
         *
         * @return
         */
        public abstract Set<E> edgesOf(V vertex);

        /**
         * .
         *
         * @param vertex
         *
         * @return
         */
        public abstract int inDegreeOf(V vertex);

        /**
         * .
         *
         * @param vertex
         *
         * @return
         */
        public abstract Set<E> incomingEdgesOf(V vertex);

        /**
         * .
         *
         * @param vertex
         *
         * @return
         */
        public abstract int outDegreeOf(V vertex);

        /**
         * .
         *
         * @param vertex
         *
         * @return
         */
        public abstract Set<E> outgoingEdgesOf(V vertex);

        /**
         * Removes the specified edge from the edge containers of its source and
         * target vertices.
         *
         * @param e
         */
        public abstract void removeEdgeFromTouchingVertices(E e);
    }

    private static class ArrayListFactory<VV, EE>
        implements EdgeSetFactory<VV, EE>,
            Serializable
    {
        private static final long serialVersionUID = 5936902837403445985L;

        /**

         */
        public Set<EE> createEdgeSet(VV vertex)
        {
            // NOTE:  use size 1 to keep memory usage under control
            // for the common case of vertices with low degree
            return new ArrayUnenforcedSet<EE>(1);
        }
    }

    
    /**
     * A container of for vertex edges.
     *
     * <p>In this edge container we use array lists to minimize memory toll.
     * However, for high-degree vertices we replace the entire edge container
     * with a direct access subclass (to be implemented).</p>
     *
     * @author Barak Naveh
     */
    private static class UndirectedEdgeContainer<VV, EE>
        implements Serializable
    {
        private static final long serialVersionUID = -6623207588411170010L;
        Set<EE> vertexEdges;
        private transient Set<EE> unmodifiableVertexEdges = null;

        UndirectedEdgeContainer(
            EdgeSetFactory<VV, EE> edgeSetFactory,
            VV vertex)
        {
            vertexEdges = edgeSetFactory.createEdgeSet(vertex);
        }

        /**
         * A lazy build of unmodifiable list of vertex edges
         *
         * @return
         */
        public Set<EE> getUnmodifiableVertexEdges()
        {
            if (unmodifiableVertexEdges == null) {
                unmodifiableVertexEdges =
                    Collections.unmodifiableSet(vertexEdges);
            }

            return unmodifiableVertexEdges;
        }

        /**
         * .
         *
         * @param e
         */
        public void addEdge(EE e)
        {
            vertexEdges.add(e);
        }

        /**
         * .
         *
         * @return
         */
        public int edgeCount()
        {
            return vertexEdges.size();
        }

        /**
         * .
         *
         * @param e
         */
        public void removeEdge(EE e)
        {
            vertexEdges.remove(e);
        }
    }

    /**
     * .
     *
     * @author Barak Naveh
     */
    private class UndirectedSpecifics
        extends Specifics
        implements Serializable
    {
        private static final long serialVersionUID = 6494588405178655873L;
        private static final String NOT_IN_UNDIRECTED_GRAPH =
            "no such operation in an undirected graph";

        private Map<V, UndirectedEdgeContainer<V, E>> vertexMapUndirected =
            new LinkedHashMap<V, UndirectedEdgeContainer<V, E>>();

        public void addVertex(V v)
        {
            // add with a lazy edge container entry
            vertexMapUndirected.put(v, null);
        }

        public Set<V> getVertexSet()
        {
            return vertexMapUndirected.keySet();
        }

        /**
         * @see Graph#getAllEdges(Object, Object)
         */
        public Set<E> getAllEdges(V sourceVertex, V targetVertex)
        {
            Set<E> edges = null;

            if (containsVertex(sourceVertex)
                && containsVertex(targetVertex))
            {
                edges = new ArrayUnenforcedSet<E>();

                Iterator<E> iter =
                    getEdgeContainer(sourceVertex).vertexEdges.iterator();

                while (iter.hasNext()) {
                    E e = iter.next();

                    boolean equalStraight =
                        sourceVertex.equals(getEdgeSource(e))
                        && targetVertex.equals(getEdgeTarget(e));

                    boolean equalInverted =
                        sourceVertex.equals(getEdgeTarget(e))
                        && targetVertex.equals(getEdgeSource(e));

                    if (equalStraight || equalInverted) {
                        edges.add(e);
                    }
                }
            }

            return edges;
        }

        /**
         * @see Graph#getEdge(Object, Object)
         */
        public E getEdge(V sourceVertex, V targetVertex)
        {
            if (containsVertex(sourceVertex)
                && containsVertex(targetVertex))
            {
                Iterator<E> iter =
                    getEdgeContainer(sourceVertex).vertexEdges.iterator();

                while (iter.hasNext()) {
                    E e = iter.next();

                    boolean equalStraight =
                        sourceVertex.equals(getEdgeSource(e))
                        && targetVertex.equals(getEdgeTarget(e));

                    boolean equalInverted =
                        sourceVertex.equals(getEdgeTarget(e))
                        && targetVertex.equals(getEdgeSource(e));

                    if (equalStraight || equalInverted) {
                        return e;
                    }
                }
            }

            return null;
        }

        /**
         *
         */
        public void addEdgeToTouchingVertices(E e)
        {
            V source = getEdgeSource(e);
            V target = getEdgeTarget(e);

            getEdgeContainer(source).addEdge(e);

            if (source != target) {
                getEdgeContainer(target).addEdge(e);
            }
        }

        /**
         * @see UndirectedGraph#degreeOf(V)
         */
        public int degreeOf(V vertex)
        {
            
            return getEdgeContainer(vertex).edgeCount();
        }

        /**
         * @see Graph#edgesOf(V)
         */
        public Set<E> edgesOf(V vertex)
        {
            return getEdgeContainer(vertex).getUnmodifiableVertexEdges();
        }

        /**
         *
         */
        public int inDegreeOf(V vertex)
        {
            throw new UnsupportedOperationException(NOT_IN_UNDIRECTED_GRAPH);
        }

        /**
         *
         */
        public Set<E> incomingEdgesOf(V vertex)
        {
            throw new UnsupportedOperationException(NOT_IN_UNDIRECTED_GRAPH);
        }

        /**
         *
         */
        public int outDegreeOf(V vertex)
        {
            throw new UnsupportedOperationException(NOT_IN_UNDIRECTED_GRAPH);
        }

        /**
         *
         */
        public Set<E> outgoingEdgesOf(V vertex)
        {
            throw new UnsupportedOperationException(NOT_IN_UNDIRECTED_GRAPH);
        }

        /**
         *
         */
        public void removeEdgeFromTouchingVertices(E e)
        {
            V source = getEdgeSource(e);
            V target = getEdgeTarget(e);

            getEdgeContainer(source).removeEdge(e);

            if (source != target) {
                getEdgeContainer(target).removeEdge(e);
            }
        }

        /**
         * A lazy build of edge container for specified vertex.
         *
         * @param vertex a vertex in this graph.
         *
         * @return EdgeContainer
         */
        private UndirectedEdgeContainer<V, E> getEdgeContainer(V vertex)
        {
            assertVertexExist(vertex);

            UndirectedEdgeContainer<V, E> ec = vertexMapUndirected.get(vertex);

            if (ec == null) {
                ec = new UndirectedEdgeContainer<V, E>(
                    edgeSetFactory,
                    vertex);
                vertexMapUndirected.put(vertex, ec);
            }

            return ec;
        }
    }
}

