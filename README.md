Traffic-efficient-Storm
=

Introduction of our undergoing project
-
Storm provides a Even-Scheduler as default scheduler, which try to fairly distribute the executors and workers to the cluster by the round-robin strategy. The default scheduler neither take the communication patterns among the tasks into account nor consider the load balance, which may lead to high processing latency and low throughput. We provide a adaptive group-based scheduler, which would try to assign the communicated tasks to one worker process, while taking into account the runtime workload at the same time. Further, we provide a new protocol for Netty, which is the IPC framework of Storm, to accelerate the IPC of workers.  

The contributions of this paper
-
1) To our best knowledge, we are the first  to propose a general framework to solve operator placement  for BDSP and implement it on current popular distribute stream procesing system. We propose our Group-based strategy for BDSP, which can group the highly-communicating operator instances together as one,  which will be assigned to the same worker process and thus reduces the inter-process traffic.   
2) We modeling the runtime relationship of operators with runtime traffic and workload. Better assignment will achieve as the  input workload vary. The model works not only for online but also for offline while many solutions in related works is only for either online or online.   
3) We design a  pre-allocated off-heap ring buffer to reduce the number of intermediate memory copies when data is transferred between two worker process, which can improve the efficiency of IPC significantly.   
4)Based on the ring buffer, we provide a novel protocol for consumer and producer when messages transferred between the worker process. This protocol can be applied to all BDSP when transferring message among worker process. 


Evaluation(work 1&2 have been implemented,the work 3&4 is undergoing)
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

