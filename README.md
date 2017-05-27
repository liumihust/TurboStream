Traffic-efficient-Storm
=

Introduction of our undergoing project
-
Storm provides a Even-Scheduler as default scheduler, which try to fairly distribute the executors and workers to the cluster by the round-robin strategy. The default scheduler neither take the communication patterns among the tasks into account nor consider the load balance, which may lead to high processing latency and low throughput. We provide a adaptive group-based scheduler, which would try to assign the communicated tasks to one worker process, while taking into account the runtime workload at the same time. Further, we provide a new protocol for  Netty, which is the IPC framework of Storm, to accelerate the IPC of workers.  

The contributions of this paper
-
1.We propose our Group-based Scheduler for scheduling a Storm topology, which can group the highly communicating tasks together as a one, which will be assigned to the same slot not just the same node and thus reduces the inter-worker and inter-node traffic.   
2.We pay our attention to both the runtime traffic patterns among the tasks and the runtime workload balance,thus provide a more efficient assignment.   
3.We will not distribute the workers to all the node, but only to the nodes selected by our algorithm to void unnecessary distribution of the topology and gain load balance in multi-topology scenario.   
4.We reduce the number of times of data copy and provide a novel protocol for consumer and producer when Netty transfers data between the workers.the improved Netty  accelerates the communication significantly. 

Evaluation
-
The experiments following is on the Yahoo workload(https://github.com/yahoo/storm-perf-test/).   
The high load scenarios,the speed of data source is about 2.5WTPS,the size of the each message is about 100B.   
We record the average processing time of each message in the topology:   
two workers in two nodes:   
![highload](https://github.com/liumihust/gitTset/blob/master/1.PNG)
two workers in one node:   
![highload](https://github.com/liumihust/gitTset/blob/master/2.PNG)  

While in the low load scenarios,the speed of data source is about 1WTPS,the size of the each message is the same 100B.
two workers in two nodes: 
![lowload](https://github.com/liumihust/gitTset/blob/master/3.PNG)  
two workers in one node:   
![lowload](https://github.com/liumihust/gitTset/blob/master/4.PNG) 

