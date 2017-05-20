package com.alibaba.jstorm.schedule.groupScheduler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.schedule.groupScheduler.graph.Graph;
import com.alibaba.jstorm.schedule.groupScheduler.graph.WeightedGraph;
import com.alibaba.jstorm.schedule.groupScheduler.graph.EdgeWeighted;
import com.alibaba.jstorm.schedule.groupScheduler.graph.SimpleWeightedGraph;
import com.alibaba.jstorm.schedule.groupScheduler.graph.UndirectedWeightedSubgraph;

import com.alibaba.jstorm.schedule.default_assign.DefaultTopologyAssignContext;
import com.alibaba.jstorm.schedule.default_assign.ResourceWorkerSlot;
import com.alibaba.jstorm.cluster.Common;



public class MultiLevelKLTaskTopologyPartitioner{

	private final static int tooBig = 10;
	private final static double coarseThreshold = 0.8;
	private double myBalanceFactor = 0.05;
	
	private int myNumPartitions;
	private WeightedGraph<Vertex, EdgeWeighted> myGraph;
	Set<Integer> tasks;
	List<ResourceWorkerSlot> workers;

	public MultiLevelKLTaskTopologyPartitioner(DefaultTopologyAssignContext context,
											   List<ResourceWorkerSlot> workers, int num ,Set<Integer> tasks){
		this.workers = workers;
		this.tasks = tasks;
		if(context.getFirstTime()){
			initialize(Common.buildSpoutOutoputAndBoltInputMap(context), context.getComponentTasks(),num);
		}else{
			StormClusterState zk = context.getNimbusData().getStormClusterState();
			for(int i = 0;i<num;i++){
				//zk.getRuntimeLoad(workers.get(0).)
				//TODO

			}





		}

	}


	
	public double getBalanceFactor() {
		return myBalanceFactor;
	}

	public void setBalanceFactor(double myBalanceFactor) {
		this.myBalanceFactor = myBalanceFactor;
	}
	
	public void initialize(WeightedGraph<Vertex, EdgeWeighted> graph,int numPartitions) {
		myGraph = graph;
		myNumPartitions = numPartitions;
	}

	public void initialize(Map<String,Set<String>> relationShip, Map<String,List<Integer>> comp2Task,
						   int numPartitions) {


		myGraph = firstTimeConvertTaskTopologyToGraph(relationShip,comp2Task);
		myNumPartitions = numPartitions;
	}

	public ArrayList<Set<Integer>> getPartitions() {
		ArrayList<Set<Vertex>> partitions = partitionGraph(myGraph, myNumPartitions);
		ArrayList<Set<Integer>> nodePartitions = new ArrayList<Set<Integer>>();
		for(Set<Vertex> part : partitions){
			Set<Integer> newPart = new HashSet<Integer>();
			for(Vertex nodeVertex : part){
				newPart.add(nodeVertex.getId());
			}
			nodePartitions.add(newPart);
		}
		return nodePartitions;
	}
		
	public Map<Integer,ResourceWorkerSlot> getWorkerAssinment() {
		ArrayList<Set<Vertex>> partitions = partitionGraph(myGraph, myNumPartitions);
		Map<Integer,ResourceWorkerSlot> task2worker = new HashMap<Integer,ResourceWorkerSlot>();
		for(int i = 0 ; i<partitions.size() ; i++){
			for(Vertex vertex :partitions.get(i)){
				task2worker.put(vertex.getId(),workers.get(i));
			}
		}
		return task2worker;
	}


	private <V extends Vertex> Set<V> getNeighbourhood(WeightedGraph<V, EdgeWeighted> graph, V vertex){
		
		Set<EdgeWeighted> edgeSet = graph.edgesOf(vertex);
		Set<V> neighbourhood = new HashSet<V>();
		
		for(EdgeWeighted curEdge : edgeSet ){
			V neighbour = graph.getEdgeSource(curEdge);
			
			if(neighbour == vertex){
				neighbour = graph.getEdgeTarget(curEdge);
			}
			
			neighbourhood.add(neighbour);
		}
		
		return neighbourhood;
	}
	
	
	private WeightedGraph<Vertex, EdgeWeighted> 
	noFirstTimeConvertTaskTopologyToWeightedGraph(HashMap<Integer,HashMap<Integer,Long>> taskTopology,HashMap<Integer,Long> weights){
		
		WeightedGraph<Vertex, EdgeWeighted> taskGraph = new SimpleWeightedGraph<Vertex, EdgeWeighted>(EdgeWeighted.class);
		//keep the record of the vertex that has been added to the graph to avoid being added twice or more
		Map<Integer,Vertex> task2Vertex = new HashMap<Integer,Vertex>();
		
		for(Map.Entry<Integer,HashMap<Integer,Long>> taskOut : taskTopology.entrySet()){
			
			int taskId = taskOut.getKey();// the task that transfer data
			Vertex v = task2Vertex.get(taskId);
			if(v == null){
				Long weight = weights.get(taskId);
				v = new Vertex(taskId,weight);
				taskGraph.addVertex(v);
				task2Vertex.put(taskId, v);
			}
			
			for(Map.Entry<Integer,Long> neighbor : taskOut.getValue().entrySet()){
				taskId = neighbor.getKey();// the tasks that receive data from v
				Vertex u = task2Vertex.get(taskId);
				if(u == null){
					Long weight = weights.get(taskId);
					u = new Vertex(taskId,weight);
					taskGraph.addVertex(u);
					task2Vertex.put(taskId, u);
				}

				EdgeWeighted edge = taskGraph.addEdge(v, u);
				taskGraph.setEdgeWeight(edge, neighbor.getValue());
			}
		}
	
		return taskGraph;
	}
	
	
	
	private WeightedGraph<Vertex, EdgeWeighted> 
	firstTimeConvertTaskTopologyToGraph(Map<String,Set<String>> relationShip,Map<String,List<Integer>> compoment2Task){
		
		WeightedGraph<Vertex, EdgeWeighted> taskGraph = new SimpleWeightedGraph<Vertex, EdgeWeighted>(EdgeWeighted.class);
		//keep the record of the vertex that has been added to the graph to avoid being added twice or more
		Map<Integer,Vertex> task2Vertex = new HashMap<Integer,Vertex>();
		
		for(Map.Entry<String,Set<String>> compomentRelated : relationShip.entrySet()){
			
			String compomentid = compomentRelated.getKey();// the task that transfer data
			List<Integer> taskids = compoment2Task.get(compomentid);
			for(Integer taskid : taskids){
				Vertex v = task2Vertex.get(taskid);
				if(v == null){
					v = new Vertex(taskid,1.0);
					taskGraph.addVertex(v);
					task2Vertex.put(taskid, v);
				}
				for(String comp : compomentRelated.getValue()){
					List<Integer> taskids2 = compoment2Task.get(comp);
					for(Integer taskid2 : taskids2){
						Vertex u = task2Vertex.get(taskid2);
						if(u == null){
							u = new Vertex(taskid2,1.0);
							taskGraph.addVertex(u);
							task2Vertex.put(taskid2, u);
						}
						EdgeWeighted edge = taskGraph.addEdge(v, u);
						if(edge != null){
							taskGraph.setEdgeWeight(edge, 1.0);
						}

					}
				}
			}	
		}
		return taskGraph;
	}
	
	
	
	
	
	
	
	private <V extends Vertex> WeightedGraph<Vertex, EdgeWeighted> 
			coarsenGraph(WeightedGraph<V, EdgeWeighted> graph){
		
		//Compute heavy-edge maximal matching.
		LinkedList<V> vertexOrder = new LinkedList<V>(graph.vertexSet());
		
		Collections.shuffle(vertexOrder);
		
		Set<V> verticesInMatching = new HashSet<V>();
		Set<EdgeWeighted> edgesInMatching = new HashSet<EdgeWeighted>();
		
		EdgeComparator edgeComparator = new EdgeComparator(graph);
		
		for(V curVertex : vertexOrder){
			
			if(!verticesInMatching.contains(curVertex)){
				
				verticesInMatching.add(curVertex);
				
				Set<EdgeWeighted> curEdges = graph.edgesOf(curVertex);
				
				LinkedList<EdgeWeighted> curEdgesList = new LinkedList<EdgeWeighted>(curEdges);
				
				Collections.sort(curEdgesList, edgeComparator);
				
				Iterator<EdgeWeighted> edgeIter = curEdgesList.iterator();
				while(edgeIter.hasNext()){
					EdgeWeighted curEdge = edgeIter.next();
					V neighbourVertex = graph.getEdgeSource(curEdge);
					
					if(neighbourVertex == curVertex){
						neighbourVertex = graph.getEdgeTarget(curEdge);
					}
					
					if(!verticesInMatching.contains(neighbourVertex)){
						// We've found an edge to add to the matching
						edgesInMatching.add(curEdge);
						verticesInMatching.add(neighbourVertex);
						break;
					}
				}
			}
		}
		
		// now use the matching to construct the coarser graph
		WeightedGraph<Vertex, EdgeWeighted> coarseGraph = 
			new SimpleWeightedGraph<Vertex, EdgeWeighted>(EdgeWeighted.class);

		// add to the coarse graph vertices which correspond to edges in the matching
		for(EdgeWeighted curEdge : edgesInMatching){
			Vertex newVertex = new Vertex();
			
			V source = graph.getEdgeSource(curEdge);
			V target = graph.getEdgeTarget(curEdge);
			
			newVertex.addSubordinate(source);
			newVertex.addSubordinate(target);
			
			coarseGraph.addVertex(newVertex);
			
			verticesInMatching.remove(source);
			verticesInMatching.remove(target);
		}
		
		// verticesInMatching now only contains lone vertices,
		// those which weren't assigned a partner in the matching :(
		for(V curVertex : verticesInMatching){
			Vertex newVertex = new Vertex();
			newVertex.addSubordinate(curVertex);
			coarseGraph.addVertex(newVertex);
		}

		// the courseGraph has all the vertices it'll ever get, now it needs the edges
		for(EdgeWeighted curEdge : graph.edgeSet()){
			Vertex parent1 = graph.getEdgeSource(curEdge).getParent();
			Vertex parent2 = graph.getEdgeTarget(curEdge).getParent();
			
			if(parent1 != parent2){
				
				double oldEdgeWeight = graph.getEdgeWeight(curEdge);
				EdgeWeighted edgeInCoarseGraph = coarseGraph.getEdge(parent1, parent2);
				
				if(edgeInCoarseGraph != null){
					coarseGraph.setEdgeWeight(edgeInCoarseGraph, coarseGraph.getEdgeWeight(edgeInCoarseGraph) + oldEdgeWeight);
				}else{
					edgeInCoarseGraph = coarseGraph.addEdge(parent1, parent2);
					coarseGraph.setEdgeWeight(edgeInCoarseGraph, oldEdgeWeight);
				}
			}
		}
		
		return coarseGraph;
	}
	
	
	private static <V extends Vertex> void applyPartition(Graph<V, EdgeWeighted> graph, Set<V> partition){
		for(Vertex v : graph.vertexSet()){
			if(partition.contains(v)){
				v.setPartition(true);
			}else{
				v.setPartition(false);
			}
		}
	}

	
	private <V extends Vertex> Set<V>
	kernighanLinPartitionImprovement(Set<V> partition, WeightedGraph<V, EdgeWeighted> graph){
		Set<V> vertexSet = graph.vertexSet();
		Set<V> sideA = new HashSet<V>(partition);
		Set<V> sideB = new HashSet<V>(graph.vertexSet());
		sideB.removeAll(sideA);

		double totalVertexWeight = 0;
		for(V v : vertexSet){
			totalVertexWeight += v.getWeight();
		}
		
		Map<V, Double> vertexCosts = new HashMap<V, Double>();
		
		double maxOverallGain = 1.0;
		
		while(maxOverallGain > 0.0){
			
			//apply the partition to the graph.
			applyPartition(graph, sideA);
			
			double sideAVertexWeight = 0;
			for(V v : sideA){
				sideAVertexWeight += v.getWeight();
			}

			Set<V> unmarkedVerticesA = new HashSet<V>(sideA);
			Set<V> unmarkedVerticesB = new HashSet<V>(sideB);
			Set<V> markedVertices = new HashSet<V>();
			
			LinkedList<VertexPairContainer<V>> maxGainsList = new LinkedList<VertexPairContainer<V>>();
			
			while(!unmarkedVerticesA.isEmpty() && !unmarkedVerticesB.isEmpty()){
				
				// Compute cost D for each vertex
				for(V curVertex : vertexSet){
					double vertexCost = 0;
					
					Set<EdgeWeighted> incidentEdges = graph.edgesOf((V) curVertex);
					
					for(EdgeWeighted curEdge : incidentEdges){
						Vertex neighbourVertex = graph.getEdgeSource(curEdge);
						
						if(neighbourVertex == curVertex){
							neighbourVertex = graph.getEdgeTarget(curEdge);
						}
						
						if(curVertex.getPartition() == neighbourVertex.getPartition()){
							vertexCost -= graph.getEdgeWeight(curEdge); 
						}else{
							vertexCost += graph.getEdgeWeight(curEdge); 
						}
					}
					
					vertexCosts.put(curVertex, vertexCost);
				}
				
				// calculate pair of unmarked vertices which gives max gain in cut quality if switched
				double maxGain = Double.NEGATIVE_INFINITY;
				V maxGainVertexA = null, maxGainVertexB = null;
				for(V curVertexA : unmarkedVerticesA){
					for(V curVertexB : unmarkedVerticesB){
						EdgeWeighted currentEdge = graph.getEdge( curVertexA, curVertexB);
						double edgeWeight = (currentEdge != null) ? graph.getEdgeWeight(currentEdge) : 0.0;
						
						double gain = vertexCosts.get(curVertexA) + vertexCosts.get(curVertexB) - 2 * edgeWeight;
						
						if(gain > maxGain){
							maxGain = gain;
							maxGainVertexA = curVertexA;
							maxGainVertexB = curVertexB;
						}
					}
				}
				
				VertexPairContainer<V> pairContainer = new VertexPairContainer<V>(maxGain, maxGainVertexA, maxGainVertexB);
				maxGainsList.addLast(pairContainer);
				
				// swap a and b in the partition applied to the graph
				// we recalculate the costs of the remaining vertices
				// as if a and b were swapped.
				maxGainVertexA.switchPartition();
				maxGainVertexB.switchPartition();
				
				unmarkedVerticesA.remove(maxGainVertexA);
				unmarkedVerticesB.remove(maxGainVertexB);
				markedVertices.add(maxGainVertexA);
				markedVertices.add(maxGainVertexB);
			}
			
			maxOverallGain = Double.NEGATIVE_INFINITY;
			double runningSum = 0.0;
			int maxOverallGainIndex = 0;
			double sideAVertexWeightAfterSwap = sideAVertexWeight;
			
			// Compute the two swap sets which will give the highest gain while
			// maintaining a good balance between the two sides of the partition.
			int j = 0;
			for(VertexPairContainer<V> pairContainer : maxGainsList){
				runningSum += pairContainer.getVal();
				
				sideAVertexWeightAfterSwap -= pairContainer.myVertexA.getWeight();
				sideAVertexWeightAfterSwap += pairContainer.myVertexB.getWeight();
				
				double balance = sideAVertexWeightAfterSwap / totalVertexWeight;
				boolean goodBalance = Math.abs(balance - 0.5) < myBalanceFactor;
				
				if(maxOverallGain < runningSum && goodBalance){
					maxOverallGain = runningSum;
					maxOverallGainIndex = j;
				}
				
				j++;
			}
			
			// If there is a swap that will give positive gain, then make it so.
			if(maxOverallGain > 0.0){
				Set<V> sideASwap = new HashSet<V>();
				Set<V> sideBSwap = new HashSet<V>();
				
				ListIterator<VertexPairContainer<V>> iter = maxGainsList.listIterator();
				
				for(j = 0; j <= maxOverallGainIndex; j++){
					
					VertexPairContainer<V> pairContainer = iter.next();
					
					sideASwap.add(pairContainer.getVertexA());
					sideBSwap.add(pairContainer.getVertexB());
				}
				
				sideA.removeAll(sideASwap);
				sideA.addAll(sideBSwap);
				
				sideB.removeAll(sideBSwap);
				sideB.addAll(sideASwap);
			}
		}
		
		return sideA;
	}


    /*
     * the return type of the multilevelKL is a Set of vertex,which is the balanced part of the graph with the minimize cut value.
     * the core algorithm is coarse and recursive
     *
     * */
    @SuppressWarnings("unchecked")
    private <V extends Vertex> Set<V> multilevelKL(WeightedGraph<V, EdgeWeighted> graph){

        Set<V> vertexSet = graph.vertexSet();
        int graphSize = vertexSet.size();

        WeightedGraph<Vertex, EdgeWeighted> coarseGraph = null;

        if(graphSize >= tooBig){
            coarseGraph = coarsenGraph(graph);
        }

        // if coarsening the graph does not significantly reduce the number of vertices,
        // or there weren't that many to begin with, then just partition the original graph
        if(graphSize < tooBig || coarseGraph.vertexSet().size() > coarseThreshold * vertexSet.size()){

            // run the BFS algorithm several times
            int numTry = Math.min(20, graphSize*2);

            double totalVertexWeight = 0;
            for(V v : vertexSet){
                totalVertexWeight += v.getWeight();
            }

            double minCut = Double.POSITIVE_INFINITY;
            Set<V> minCutPartition = new HashSet<V>();

            //if there is no balance partition,the minCutPartition will be empty
            //this is a bug,so I fix it by record the best balance partition in the unbalance partitons;
            double rest_bestBalance = Double.POSITIVE_INFINITY;
            Set<V> rest_partition = new HashSet<V>();



            List<V> startVertexList = new LinkedList<V>(vertexSet);
            Collections.shuffle(startVertexList);

            List<V>  tmp = new LinkedList<V>(startVertexList);
            Collections.shuffle(startVertexList);
            tmp.addAll(startVertexList);
            ListIterator<V> startVertexIter = tmp.listIterator();
            //ListIterator<V> startVertexIter = startVertexList.listIterator();


            for(int i = 0; i < numTry; i++){

                V startVertex = startVertexIter.next();

                // start with a different random vertex on each rep,
                // run BFS and see which gives best partition
                double partitionVertexWeight = 0.0;
                double balance = 0.0;

                PriorityQueue<V> queue = new PriorityQueue<V>();
                Set<V> partition = new HashSet<V>();
                Set<V> checked = new HashSet<V>();
                LinkedList<V> unchecked = new LinkedList<V>(vertexSet);
                Collections.shuffle(unchecked);

                queue.add(startVertex);

                while(!queue.isEmpty() && balance < 0.5){
                    V curVertex = queue.poll();
                    double curVertexWeight = curVertex.getWeight();

                    double balanceWithCurrentVertex = (partitionVertexWeight + curVertexWeight) / totalVertexWeight;
                    boolean betterBalance = Math.abs(balanceWithCurrentVertex - 0.5) < Math.abs(balance - 0.5);

                    // if adding the current head of the queue to
                    // the partition would give a better balance, then do it
                    if(betterBalance){
                        partition.add(curVertex);

                        partitionVertexWeight += curVertexWeight;

                        balance = partitionVertexWeight / totalVertexWeight;

                        Set<V> neighbourhood = getNeighbourhood(graph, curVertex);

                        neighbourhood.removeAll(checked);

                        queue.addAll(neighbourhood);
                    }

                    checked.add(curVertex);
                    unchecked.remove(curVertex);

                    // if the queue is empty but we don't yet have a good balance, randomly choose a vertex
                    // we haven't visited yet and start from there (still on the same try)
                    if(queue.isEmpty() && !unchecked.isEmpty() && (Math.abs(balance - 0.5) > myBalanceFactor)){
                        queue.add(unchecked.get(0));
                    }
                }//the end of the while loop,means the balance partition has been found. next,to check the total cut value.



                // compare current partition to the best so far, as long as current partition has decent balance
                // note:only the balanced partition has the chance to compare!!
                if(Math.abs(balance - 0.5) < myBalanceFactor){
                    // find the cut value of the partition found on this try
                    double cutValue = 0.0;
                    for(EdgeWeighted curEdge : graph.edgeSet()){
                        V source = graph.getEdgeSource(curEdge);
                        V target = graph.getEdgeTarget(curEdge);

                        boolean sourceInPartition = partition.contains(source);
                        boolean targetInPartition = partition.contains(target);

                        if(sourceInPartition != targetInPartition){
                            cutValue += graph.getEdgeWeight(curEdge);
                        }
                    }

                    if(cutValue < minCut){
                        minCut = cutValue;
                        minCutPartition = partition;
                    }
                }else{//un-balance partition shuoldn't be dropped
                    if(Math.abs(balance - 0.5) < rest_bestBalance){
                        rest_partition = partition;
                        rest_bestBalance = Math.abs(balance - 0.5);
                    }
                }
            }

            if(minCutPartition.size() == 0){
                System.out.println("hahhha");
                return rest_partition;
            }
            return minCutPartition;

        }else{

            Set<Vertex> coarsePartition = multilevelKL(coarseGraph);

            // Use partition on coarse graph to find a partition of the original graph
            Set<V> partition = new HashSet<V>();

            for(Vertex v : coarsePartition){
                Set<Vertex> subordinates = v.getSubordinates();

                for(Vertex subord : subordinates){

                    //Could get rid of this cast by adding a type parameter to Vertex class
                    partition.add((V) subord);
                }
            }

            partition = kernighanLinPartitionImprovement(partition, graph);

            return partition;
        }
    }
	
	
	
	
	private <V extends Vertex> ArrayList<Set<V>> 
		partitionGraph(WeightedGraph<V, EdgeWeighted> graph, int numPartitions){
		
		ArrayList<Set<V>> partitions = new ArrayList<Set<V>>();
		
		if(numPartitions < 1){
			return partitions;
		}else if(graph.vertexSet().size() <= numPartitions){
			
			// In this case there is no point in computing min cut, 
			//just assign each node to a different partition.
			Iterator<V> nodeIter = graph.vertexSet().iterator();
			for(int i = 0; i < numPartitions; i++){
				Set<V> newSet = new HashSet<V>();
				
				if(nodeIter.hasNext()){
					newSet.add(nodeIter.next());
				}
				
				partitions.add(newSet);
			}
			
			return partitions;
			
		}else if(numPartitions == 1){
			partitions.add(graph.vertexSet());
			return partitions;
		}
		
		Set<V> partition = multilevelKL(graph);
		
		Set<V> leftSubgraphVertexSet = partition;
		Set<V> rightSubgraphVertexSet = new HashSet<V>(graph.vertexSet());
		for(V node : leftSubgraphVertexSet){
			rightSubgraphVertexSet.remove(node);
		}
		
		// swap to make sure left partition is the larger one.
		
		if(leftSubgraphVertexSet.size() < rightSubgraphVertexSet.size()){
			Set<V> temp = rightSubgraphVertexSet;
			rightSubgraphVertexSet = leftSubgraphVertexSet;
			leftSubgraphVertexSet = temp;
		}
		
		
		
		if(numPartitions == 2){
			partitions.add(leftSubgraphVertexSet);
			partitions.add(rightSubgraphVertexSet);
			return partitions;
		}

		Iterator<EdgeWeighted> edgeIter = graph.edgeSet().iterator();
		
		Set<EdgeWeighted> leftSubgraphEdgeSet = new HashSet<EdgeWeighted>();
		Set<EdgeWeighted> rightSubgraphEdgeSet = new HashSet<EdgeWeighted>();
		
		while(edgeIter.hasNext()){
			EdgeWeighted edge = edgeIter.next();
			
			Vertex source = graph.getEdgeSource(edge);
			Vertex target = graph.getEdgeTarget(edge);
			
			boolean sourceInLeft = leftSubgraphVertexSet.contains(source);
			boolean targetInLeft = leftSubgraphVertexSet.contains(target);
			
			if(sourceInLeft && targetInLeft){
				leftSubgraphEdgeSet.add(edge);
			}else if(!sourceInLeft && !targetInLeft){
				rightSubgraphEdgeSet.add(edge);
			}
		}

		WeightedGraph<V, EdgeWeighted> leftSubgraph, rightSubgraph;
		
		leftSubgraph = 
			new UndirectedWeightedSubgraph<V, EdgeWeighted>(graph, leftSubgraphVertexSet, leftSubgraphEdgeSet);
		
		rightSubgraph = 
			new UndirectedWeightedSubgraph<V, EdgeWeighted>(graph, rightSubgraphVertexSet, rightSubgraphEdgeSet);
		
		int numLeftSubPartitions = (int) Math.ceil((double) numPartitions / 2), 
			numRightSubPartitions = (int) Math.floor((double) numPartitions / 2);
		
		ArrayList<Set<V>> leftPartitions = partitionGraph(leftSubgraph, numLeftSubPartitions);
		ArrayList<Set<V>> rightPartitions = partitionGraph(rightSubgraph, numRightSubPartitions);
		
		partitions.addAll(leftPartitions);
		partitions.addAll(rightPartitions);
		return partitions;
	}
	
	
	
	public static class Vertex implements Comparable<Vertex>{
		
		int vertexId;
		double myWeight;
		boolean myPartition;
		//record the relationship between the coarse graph and the graph
		Set<Vertex> mySubordinates;
		Vertex myParent;
		
		public Vertex(){
			myWeight = 0;
			mySubordinates = new HashSet<Vertex>();
		}
		
		public Vertex(int id ,double weight){
			vertexId = id;
			myWeight = weight;
			mySubordinates = new HashSet<Vertex>();
		}
		
		public double getWeight(){
			return myWeight;
		}
		
		public void addSubordinate(Vertex sub){
			sub.setParent(this);
			mySubordinates.add(sub);
			myWeight += sub.getWeight();
		}
		
		public int getId() {
			return vertexId;
		}
		
		public Set<Vertex> getSubordinates(){
			return mySubordinates;
		}
		
		public Vertex getParent() {
			return myParent;
		}
		
		public void setParent(Vertex parent) {
			this.myParent = parent;
		}
		
		public boolean getPartition(){
			return myPartition;
		}
		
		public void setPartition(boolean partition){
			myPartition = partition;
		}
		
		public void switchPartition(){
			myPartition = !myPartition;
		}
		
		public int compareTo(Vertex v){
			double weight1 = this.getWeight();
			double weight2 = v.getWeight();
				
			if(weight1 > weight2)
				return 1;
			else if(weight1 < weight2)
				return -1;
			else
				return 0;
		}
	}
	
	
	

	private static class EdgeComparator implements Comparator<EdgeWeighted>{
		
		WeightedGraph<? extends Vertex, EdgeWeighted> myGraph;
		
		public EdgeComparator(WeightedGraph<? extends Vertex, EdgeWeighted> graph){
			myGraph = graph;
		}
		
		public int compare(EdgeWeighted e1, EdgeWeighted e2){
			double weight1 = myGraph.getEdgeWeight(e1);
			double weight2 = myGraph.getEdgeWeight(e2);
			
			if(weight1 > weight2)
				return 1;
			else if(weight1 < weight2)
				return -1;
			else
				return 0; 
		}
	}
	
	
	private static class VertexPairContainer <V extends Vertex>{
		private double myVal;
		private V myVertexA;
		private V myVertexB;
		
		public VertexPairContainer(double val, V vertexA, V vertexB){
			myVal = val;
			myVertexA = vertexA;
			myVertexB = vertexB;
		}
		
		public double getVal() {
			return myVal;
		}

		public V getVertexA() {
			return myVertexA;
		}

		public V getVertexB() {
			return myVertexB;
		}
	}
	
}
