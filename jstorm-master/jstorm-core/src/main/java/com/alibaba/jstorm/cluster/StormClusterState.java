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
package com.alibaba.jstorm.cluster;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.daemon.supervisor.SupervisorInfo;
import com.alibaba.jstorm.schedule.Assignment;
import com.alibaba.jstorm.schedule.AssignmentBak;
import com.alibaba.jstorm.task.TaskInfo;
import com.alibaba.jstorm.task.error.TaskError;
import com.alibaba.jstorm.task.backpressure.SourceBackpressureInfo;
import com.alibaba.jstorm.utils.Pair;

import backtype.storm.generated.TopologyTaskHbInfo;

/**
 * all storm in zk operation interface
 */
public interface StormClusterState {

    public HashMap<Integer,HashMap<Integer,Integer>> getRuntimeTraffic(String workerid, RunnableCallback callback) throws Exception;
    public void setRuntimeTraffic(String workerid, HashMap<Integer,HashMap<Integer,Integer>> traffic) throws Exception;
    public HashMap<Integer,Integer> getRuntimeLoad(String workerid, RunnableCallback callback) throws Exception;
    public void setRuntimeLoad(String workerid,HashMap<Integer,Integer> load) throws Exception;


    public void disconnect() throws Exception;

    public void remove_storm(String topology_id) throws Exception;

    public void try_remove_storm(String topology_id);

    public List<String> assignments(RunnableCallback callback) throws Exception;

    public Assignment assignment_info(String topology_id, RunnableCallback callback) throws Exception;

    public Integer assignment_version(String topology_id, RunnableCallback callback) throws Exception;

    public void set_assignment(String topology_id, Assignment info) throws Exception;

    public AssignmentBak assignment_bak(String topologyName) throws Exception;

    public void backup_assignment(String topology_id, AssignmentBak info) throws Exception;

    public List<String> active_storms() throws Exception;

    public StormBase storm_base(String topology_id, RunnableCallback callback) throws Exception;

    public void activate_storm(String topology_id, StormBase storm_base) throws Exception;

    public void update_storm(String topology_id, StormStatus new_elems) throws Exception;

    public void set_storm_monitor(String topologyId, boolean isEnable) throws Exception;

    public void remove_storm_base(String topology_id) throws Exception;

    public List<String> task_storms() throws Exception;

    public Set<Integer> task_ids(String topology_id) throws Exception;

    public Set<Integer> task_ids_by_componentId(String topologyId, String componentId) throws Exception;

    public void set_task(String topologyId, Map<Integer, TaskInfo> taskInfoMap) throws Exception;

    public void add_task(String topology_id, Map<Integer, TaskInfo> taskInfoMap) throws Exception;

    public void remove_task(String topologyId, Set<Integer> taskIds) throws Exception;

    public Map<Integer, TaskInfo> task_all_info(String topology_id) throws Exception;

    public List<String> heartbeat_storms() throws Exception;

    public void topology_heartbeat(String topology_id, TopologyTaskHbInfo info) throws Exception;

    public TopologyTaskHbInfo topology_heartbeat(String topologyId) throws Exception;

    public void teardown_heartbeats(String topology_id) throws Exception;

    public List<String> task_error_storms() throws Exception;

    public List<String> task_error_ids(String topologyId) throws Exception;

    public void report_task_error(String topology_id, int task_id, Throwable error) throws Exception;

    public void report_task_error(String topology_id, int task_id, String error) throws Exception;

    public void report_task_error(String topology_id, int task_id, String error, String error_level, int error_code) throws Exception;

    public void report_task_error(String topology_id, int task_id, String error, String error_level, int error_code, int duration) throws Exception;

    public void report_task_error(String topology_id, int task_id, String error, String error_level, int error_code, int duration, String tag) throws Exception;

    public Map<Integer, String> topo_lastErr_time(String topologyId) throws Exception;

    public void remove_lastErr_time(String topologyId) throws Exception;

    public List<TaskError> task_errors(String topology_id, int task_id) throws Exception;

    public void remove_task_error(String topologyId, int taskId) throws Exception;

    public List<String> task_error_time(String topologyId, int taskId) throws Exception;

    public TaskError task_error_info(String topologyId, int taskId, long timeStamp) throws Exception;

    public void teardown_task_errors(String topology_id) throws Exception;

    public List<String> supervisors(RunnableCallback callback) throws Exception;

    public SupervisorInfo supervisor_info(String supervisor_id) throws Exception;

    public void supervisor_heartbeat(String supervisor_id, SupervisorInfo info) throws Exception;

    public boolean try_to_be_leader(String path, String host, RunnableCallback callback) throws Exception;

    public String get_leader_host() throws Exception;

    public boolean leader_existed() throws Exception;

    public List<String> get_nimbus_slaves() throws Exception;

    public void update_nimbus_slave(String host, int time) throws Exception;

    public String get_nimbus_slave_time(String host) throws Exception;

    public void unregister_nimbus_host(String host) throws Exception;

    public void update_nimbus_detail(String hostPort, Map map) throws Exception;

    public Map get_nimbus_detail(String hostPort, boolean watch) throws Exception;

    public void unregister_nimbus_detail(String hostPort) throws Exception;

    public void set_topology_metric(String topologyId, Object metric) throws Exception;

    public Object get_topology_metric(String topologyId) throws Exception;

    public List<String> get_metrics() throws Exception;

    public List<String> list_dirs(String path, boolean watch) throws  Exception;

    public List<String> backpressureInfos() throws Exception;

    public void set_backpressure_info(String topologyId, Map<String, SourceBackpressureInfo> sourceToBackpressureInfo) throws Exception;
    
    public Map<String, SourceBackpressureInfo> get_backpressure_info(String topologyId) throws Exception;

    public void teardown_backpressure(String topologyId) throws Exception;
}