Towards Low-Latency Data Stream Processing
=

Introduction of our undergoing project
-

The contributions of this project
-
1) We conduct a deep investigation about the latency of each stage of DSP topology, and find the proportion of inter-operator latency in total event processing latency is up to 86.88\%.
	
2) We design the OSRingBuffer in IPC to reduce the times of memory copy and the waiting time of each single message when transmitting messages between the workers inside one node. Thus, the end-to-end latency of IPC decreases by 45.94\%(at least). To the best of our knowledge, we are the first to use the off-heap ring bytebuffer to accelerate the message transmission between JVM.
	
3) We further propose a general Group-based modeling framework, which uses the data dependencies in topology or the runtime traffic information to integrate the communicating operator instances before scheduling and BFS-based algorithm to assign the integrated operator instances. With this framework, scheduler can achieve both the load balance and the reduction in inter-node traffic.
	
4) With OSRingBuffer in IPC and the Group-based modeling framework, we integrate them into JStorm, termed TurboStream. In our experiments, the total event processing latency can decrease by 77.84\%.


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

