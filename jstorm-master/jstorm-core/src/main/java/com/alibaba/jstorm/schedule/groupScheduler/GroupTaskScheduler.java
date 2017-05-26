/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.jstorm.schedule.groupScheduler;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.schedule.TopologyAssignContext;
import com.alibaba.jstorm.schedule.default_assign.DefaultTopologyAssignContext;
import com.alibaba.jstorm.schedule.default_assign.ResourceWorkerSlot;
import com.alibaba.jstorm.schedule.default_assign.TaskAssignContext;
import com.alibaba.jstorm.utils.FailedAssignTopologyException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.Map.Entry;

public class GroupTaskScheduler {

    public static Logger LOG = LoggerFactory.getLogger(GroupTaskScheduler.class);

    private final TaskAssignContext taskContext;

    private List<ResourceWorkerSlot> assignments = new ArrayList<ResourceWorkerSlot>();

    private int workerNum;

    private Set<Integer> tasks;

    private DefaultTopologyAssignContext context;

    MultiLevelTaskTopologyGrouping partitioner;// = new MultiLevelKLTaskTopologyPartitioner();
    private Map<Integer,ResourceWorkerSlot> findWorkers;
    private List<ResourceWorkerSlot> workers;


    public GroupTaskScheduler(DefaultTopologyAssignContext context, Set<Integer> tasks, List<ResourceWorkerSlot> workers) {
        this.tasks = tasks;
        this.workers = workers;
        workerNum = workers.size();
        LOG.info("Tasks " + tasks + " is going to be assigned in workers " + workers);
        this.context = context;
        this.taskContext =
                new TaskAssignContext(this.buildSupervisorToWorker(workers), Common.buildSpoutOutoputAndBoltInputMap(context), context.getTaskToComponent());
        if (tasks.size() == 0)
            return;
        if (context.getAssignType() != TopologyAssignContext.ASSIGN_TYPE_REBALANCE || context.isReassign() != false){
            // warning ! it doesn't consider HA TM now!!
            if (context.getAssignSingleWorkerForTM() && tasks.contains(context.getTopologyMasterTaskId())) {
                workerNum--;//the last worker will be reserved for TM
                assignForTopologyMaster();
            }
        }

        partitioner = new MultiLevelTaskTopologyGrouping(context, workers, workerNum, tasks);
        findWorkers = partitioner.getWorkerAssinment();

        //after calculate the task the assignment ,first to assign the TM,then the assign() will be called
        //to assign the normal tasks.


        // For Scale-out case, the old assignment should be kept.
        if (context.getAssignType() == TopologyAssignContext.ASSIGN_TYPE_REBALANCE && context.isReassign() == false) {
            keepAssignment(tasks.size(), context.getOldAssignment().getWorkers());
        }

    }

    private void keepAssignment(int taskNum, Set<ResourceWorkerSlot> keepAssignments) {
        Set<Integer> keepTasks = new HashSet<Integer>();
        ResourceWorkerSlot tmWorker = null;
        for (ResourceWorkerSlot worker : keepAssignments) {
            if (worker.getTasks().contains(context.getTopologyMasterTaskId()))
                tmWorker = worker;
            for (Integer taskId : worker.getTasks()) {
                if (tasks.contains(taskId)) {
                    ResourceWorkerSlot contextWorker = taskContext.getWorker(worker);
                    if (contextWorker != null) {
                        if (tmWorker != null && tmWorker.getTasks().contains(taskId) && context.getAssignSingleWorkerForTM() ) {
                            if (context.getTopologyMasterTaskId() == taskId){
                                updateAssignedTasksOfWorker(taskId, contextWorker);
                                taskContext.getWorkerToTaskNum().remove(contextWorker);
                                contextWorker.getTasks().clear();
                                contextWorker.getTasks().add(taskId);
                                assignments.add(contextWorker);
                                tasks.remove(taskId);
                                taskNum--;
                                workerNum--;
                                LOG.info("assignForTopologyMaster: " + contextWorker);
                            }
                        }else {
                            String componentName = context.getTaskToComponent().get(taskId);
                            updateAssignedTasksOfWorker(taskId, contextWorker);
                            updateComponentsNumOfWorker(componentName, contextWorker);
                            keepTasks.add(taskId);
                        }
                    }
                }
            }
        }
        if ( tmWorker != null){
            keepAssignments.remove(tmWorker);
        }

        // Try to find the workers which have been assigned too much tasks
        // If found, remove the workers from worker resource pool and update
        // the avgNum and leftNum
        int doneAssignedTaskNum = 0;
        while (true) {
            boolean found = false;
            Set<ResourceWorkerSlot> doneAssignedWorkers = new HashSet<ResourceWorkerSlot>();
            for (ResourceWorkerSlot worker : keepAssignments) {
                ResourceWorkerSlot contextWorker = taskContext.getWorker(worker);
                if (contextWorker != null && isTaskFullForWorker(contextWorker)) {
                    found = true;
                    workerNum--;
                    taskContext.getWorkerToTaskNum().remove(contextWorker);
                    assignments.add(contextWorker);

                    doneAssignedWorkers.add(worker);
                    doneAssignedTaskNum += contextWorker.getTasks().size();
                }
            }

            if (found) {
                taskNum -= doneAssignedTaskNum;
                keepAssignments.removeAll(doneAssignedWorkers);
            } else {
                break;
            }
        }
        tasks.removeAll(keepTasks);
        LOG.info("keep following assignment, " + assignments);
    }

    private boolean isTaskFullForWorker(ResourceWorkerSlot worker) {
        boolean ret = false;
        //TODO
        return ret;
    }

    public List<ResourceWorkerSlot> assign() {
        if (tasks.size() == 0) {
            assignments.addAll(workers);
            return assignments;
        }

        Map<Integer, String> systemTasks = new HashMap<Integer, String>();
        for (Integer task : tasks) {
            String name = context.getTaskToComponent().get(task);
            if (Common.isSystemComponent(name)) {
                systemTasks.put(task, name);
                continue;
            }
            assignForTask(name, task);
        }
        
        /*
         * At last, make the assignment for system component, e.g. acker, topology master...
         */
        for (Entry<Integer, String> entry : systemTasks.entrySet()) {
            assignForTask(entry.getValue(), entry.getKey());
        }

        assignments.addAll(workers);
        return assignments;
    }

    private void assignForTopologyMaster() {
        int taskId = context.getTopologyMasterTaskId();

        // assign the last worker to the TM
        ResourceWorkerSlot workerAssigned = workers.get(workers.size()-1);
        if (workerAssigned == null)
            throw new FailedAssignTopologyException("there's no enough workers for the assignment of topology master");
        updateAssignedTasksOfWorker(taskId, workerAssigned);
        //taskContext.getWorkerToTaskNum().remove(workerAssigned);
        //assignments.add(workerAssigned);
        tasks.remove(taskId);
        //workerNum--;
        LOG.info("assignForTopologyMaster, assignments=" + assignments);
    }

    private void assignForTask(String name, Integer task) {
        ResourceWorkerSlot worker = chooseWorker(task,name);
        pushTaskToWorker(task, name, worker);
    }

    private Map<String, List<ResourceWorkerSlot>> buildSupervisorToWorker(List<ResourceWorkerSlot> workers) {
        Map<String, List<ResourceWorkerSlot>> supervisorToWorker = new HashMap<String, List<ResourceWorkerSlot>>();
        for (ResourceWorkerSlot worker : workers) {
                List<ResourceWorkerSlot> supervisor = supervisorToWorker.get(worker.getNodeId());
            if (supervisor == null) {
                supervisor = new ArrayList<ResourceWorkerSlot>();
                supervisorToWorker.put(worker.getNodeId(), supervisor);
            }
            supervisor.add(worker);
        }
        this.workerNum = workers.size();
        return supervisorToWorker;
    }

    private ResourceWorkerSlot chooseWorker(Integer task, String name) {
        if (Common.isSystemComponent(name))
            return workers.get(workerNum-1);

        return findWorkers.get(task);
    }

    private void pushTaskToWorker(Integer task, String name, ResourceWorkerSlot worker) {
        LOG.debug("Push task-" + task + " to worker-" + worker.getPort());
        updateAssignedTasksOfWorker(task, worker);
        updateComponentsNumOfWorker(name, worker);
    }

    private int updateAssignedTasksOfWorker(Integer task, ResourceWorkerSlot worker) {
        int ret = 0;
        Set<Integer> tasks = worker.getTasks();
        if (tasks == null) {
            tasks = new HashSet<Integer>();
            worker.setTasks(tasks);
        }
        tasks.add(task);

        ret = taskContext.getWorkerToTaskNum().get(worker);
        taskContext.getWorkerToTaskNum().put(worker, ++ret);
        return ret;
    }

    private void updateComponentsNumOfWorker(String name, ResourceWorkerSlot worker) {
        Map<String, Integer> components = taskContext.getWorkerToComponentNum().get(worker);
        if (components == null) {
            components = new HashMap<String, Integer>();
            taskContext.getWorkerToComponentNum().put(worker, components);
        }
        Integer componentNum = components.get(name);
        if (componentNum == null) {
            componentNum = 0;
        }
        components.put(name, ++componentNum);
    }
}
