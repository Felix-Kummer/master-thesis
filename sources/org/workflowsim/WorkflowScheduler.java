/**
 * Copyright 2012-2013 University Of Southern California
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.workflowsim;

import java.util.*;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.lists.VmList;
import org.workflowsim.failure.FailureGenerator;
import org.workflowsim.scheduling.DataAwareSchedulingAlgorithm;
import org.workflowsim.scheduling.BaseSchedulingAlgorithm;
import org.workflowsim.scheduling.FCFSSchedulingAlgorithm;
import org.workflowsim.scheduling.MCTSchedulingAlgorithm;
import org.workflowsim.scheduling.MaxMinSchedulingAlgorithm;
import org.workflowsim.scheduling.MinMinSchedulingAlgorithm;
import org.workflowsim.scheduling.RoundRobinSchedulingAlgorithm;
import org.workflowsim.scheduling.StaticSchedulingAlgorithm;
import org.workflowsim.utils.Parameters;
import org.workflowsim.utils.Parameters.SchedulingAlgorithm;

import federatedSim.PartitioningScheduler;
import federatedSim.RandomPartitioningScheduler;
import federatedSim.ThresholdException;
import federatedSim.utils.DisjointSetUnion;

import static org.workflowsim.utils.Parameters.SchedulingAlgorithm.DYNAMIC_PART;
import static org.workflowsim.utils.Parameters.SchedulingAlgorithm.DYNAMIC_RND;

/**
 * WorkflowScheduler represents a algorithm acting on behalf of a user. It hides
 * VM management, as vm creation, sumbission of jobs to this VMs and destruction
 * of VMs.
 * It picks up a scheduling algorithm based on the configuration
 *
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Apr 9, 2013
 */
public class WorkflowScheduler extends DatacenterBroker {

    /**
     * The workflow engine id associated with this workflow algorithm.
     */
    private int workflowEngineId;

    /**
     * Created a new WorkflowScheduler object.
     *
     * @param name name to be associated with this entity (as required by
     * Sim_entity class from simjava package)
     * @throws Exception the exception
     * @pre name != null
     * @post $none
     */
    public WorkflowScheduler(String name) throws Exception {
        super(name);
    }

    /**
     * Binds this scheduler to a datacenter
     */
    public void bindSchedulerDatacenter(int datacenterId) {
        if (datacenterId <= 0) {
            Log.printLine("Error in data center id");
            return;
        }
        this.datacenterIdsList.add(datacenterId);
    }
    
    /**
     * Sets the workflow engine id
     *
     * @param workflowEngineId the workflow engine id
     */
    public void setWorkflowEngineId(int workflowEngineId) {
        this.workflowEngineId = workflowEngineId;
    }

    /**
     * Process an event
     *
     * @param ev a simEvent obj
     */
    @Override
    public void processEvent(SimEvent ev) {
        switch (ev.getTag()) {
            // Resource characteristics request
            case CloudSimTags.RESOURCE_CHARACTERISTICS_REQUEST:
                processResourceCharacteristicsRequest(ev);
                break;
            // Resource characteristics answer
            case CloudSimTags.RESOURCE_CHARACTERISTICS:
                processResourceCharacteristics(ev);
                break;
            // VM Creation answer
            case CloudSimTags.VM_CREATE_ACK:
                processVmCreate(ev);
                break;
            // A finished cloudlet returned
            case WorkflowSimTags.CLOUDLET_CHECK:
                processCloudletReturn(ev);
                break;
            case CloudSimTags.CLOUDLET_RETURN:
                processCloudletReturn(ev);
                break;
            // if the simulation finishes
            case CloudSimTags.END_OF_SIMULATION:
                shutdownEntity();
                break;
            case CloudSimTags.CLOUDLET_SUBMIT:
                processCloudletSubmit(ev);
                break;

            case WorkflowSimTags.CLOUDLET_UPDATE:
                processCloudletUpdate(ev);
                break;

            case WorkflowSimTags.CLOUDLET_STORAGE_EXCEEDED:
                processCloudletOutputExceededStorage(ev);
                break;
            // other unknown tags are processed by this method
            default:
                processOtherEvent(ev);
                break;
        }
    }

    /**
     * Switch between multiple schedulers. Based on algorithm.method
     *
     * @param name the SchedulingAlgorithm name
     * @return the algorithm that extends BaseSchedulingAlgorithm
     */
    private BaseSchedulingAlgorithm getScheduler(SchedulingAlgorithm name) {
        BaseSchedulingAlgorithm algorithm = null;

        // choose which algorithm to use. Make sure you have add related enum in
        //Parameters.java
        switch (name) {
            //by default it is Static
            case FCFS:
                algorithm = new FCFSSchedulingAlgorithm();
                break;
            case MINMIN:
                algorithm = new MinMinSchedulingAlgorithm();
                break;
            case MAXMIN:
                algorithm = new MaxMinSchedulingAlgorithm();
                break;
            case MCT:
                algorithm = new MCTSchedulingAlgorithm();
                break;
            case DATA:
                algorithm = new DataAwareSchedulingAlgorithm();
                break;
            case STATIC:
                algorithm = new StaticSchedulingAlgorithm();
                break;
            case ROUNDROBIN:
                algorithm = new RoundRobinSchedulingAlgorithm();
                break;
            case DYNAMIC_RND:
                algorithm = new RandomPartitioningScheduler();
                break;
            case DYNAMIC_PART:
                algorithm = new PartitioningScheduler();
                break;
            default:
                algorithm = new StaticSchedulingAlgorithm();
                break;

        }

        return algorithm;
    }

    /**
     * Process the ack received due to a request for VM creation.
     *
     * @param ev a SimEvent object
     * @pre ev != null
     * @post $none
     */
    @Override
    protected void processVmCreate(SimEvent ev) {
        int[] data = (int[]) ev.getData();
        int datacenterId = data[0];
        int vmId = data[1];
        int result = data[2];

        if (result == CloudSimTags.TRUE) {
            getVmsToDatacentersMap().put(vmId, datacenterId);
            /**
             * Fix a bug of cloudsim Don't add a null to getVmsCreatedList()
             * June 15, 2013
             */
            if (VmList.getById(getVmList(), vmId) != null) {
                getVmsCreatedList().add(VmList.getById(getVmList(), vmId));

                Log.printLine(CloudSim.clock() + ": " + getName() + ": VM #" + vmId
                        + " has been created in Datacenter #" + datacenterId + ", Host #"
                        + VmList.getById(getVmsCreatedList(), vmId).getHost().getId());
            }
        } else {
            Log.printLine(CloudSim.clock() + ": " + getName() + ": Creation of VM #" + vmId
                    + " failed in Datacenter #" + datacenterId);
        }

        incrementVmsAcks();

        // all the requested VMs have been created
        if (getVmsCreatedList().size() == getVmList().size() - getVmsDestroyed()) {
            submitCloudlets();
        } else {
            // all the acks received, but some VMs were not created
            if (getVmsRequested() == getVmsAcks()) {
                // find id of the next datacenter that has not been tried
                for (int nextDatacenterId : getDatacenterIdsList()) {
                    if (!getDatacenterRequestedIdsList().contains(nextDatacenterId)) {
                        createVmsInDatacenter(nextDatacenterId);
                        return;
                    }
                }

                // all datacenters already queried
                if (getVmsCreatedList().size() > 0) { // if some vm were created
                    submitCloudlets();
                } else { // no vms created. abort
                    Log.printLine(CloudSim.clock() + ": " + getName()
                            + ": none of the required VMs could be created. Aborting");
                    finishExecution();
                }
            }
        }
    }
    
    // Bookkeeping for threshold checks, stores the last time we partitioned
    private double lastPartitioningTime = 0.0;
    

    /**
     * Update a cloudlet (job)
     *
     * @param ev a simEvent object
     */
    protected void processCloudletUpdate(SimEvent ev) {

        BaseSchedulingAlgorithm scheduler = getScheduler(Parameters.getSchedulingAlgorithm());
        scheduler.setCloudletList(getCloudletList());
        scheduler.setVmList(getVmsCreatedList());
        scheduler.setReceivedList(getCloudletReceivedList());

        // federated partitioning related options - add more if applicable

        // dynamic random
        if (Parameters.getSchedulingAlgorithm() == DYNAMIC_RND) {
            // let scheduling algo know which task exceeded which storage
            ((RandomPartitioningScheduler) scheduler).addExceededMap(exceededStorageMap);
            ((RandomPartitioningScheduler) scheduler).addRetriedJobsSetUnion(retriedJobIdsSetUnion);

        // dynamic partitioning
        } else if (Parameters.getSchedulingAlgorithm() == DYNAMIC_PART){
            // let scheduling algo know which task exceeded which storage
            ((PartitioningScheduler) scheduler).addExceededMap(exceededStorageMap);
            ((PartitioningScheduler) scheduler).addRetriedJobsSetUnion(retriedJobIdsSetUnion);
        }


        try {
        	if (Parameters.thresholds_enabled()) { // Threshold-based Scheduling
        		if (getCloudletList().size() < Parameters.getTASK_THRESHOLD()) { // check task threshold
        			if (CloudSim.clock() - lastPartitioningTime < Parameters.getSEC_THRESHOLD()) { // check time threshold
                        // send message to check thresholds again with delay defined in Parameters
                        schedule(
                            this.getId(), 
                            Parameters.getTHRESHOLD_CHECKING_INTERVAL(), 
                            WorkflowSimTags.CLOUDLET_UPDATE
                        );
                        // throw an exception to signal that no threshold was surpassed
                        throw new ThresholdException();
                    }
                }
            }
            scheduler.run();
            lastPartitioningTime = CloudSim.clock();
            
        } catch (ThresholdException e) {
        	//Log.printLine(e.getMessage());
        } catch (Exception e) {
            Log.printLine("Error in configuring scheduler_method");
            e.printStackTrace();
        } 
        List scheduledList = scheduler.getScheduledList();
        for (Iterator it = scheduledList.iterator(); it.hasNext();) {
            Cloudlet cloudlet = (Cloudlet) it.next();
            int vmId = cloudlet.getVmId();
            double delay = 0.0;
            if(Parameters.getOverheadParams().getQueueDelay()!=null){
                delay = Parameters.getOverheadParams().getQueueDelay(cloudlet);
            }
            schedule(getVmsToDatacentersMap().get(vmId), delay, CloudSimTags.CLOUDLET_SUBMIT, cloudlet);

        }
        getCloudletList().removeAll(scheduledList);
        getCloudletSubmittedList().addAll(scheduledList);
        cloudletsSubmitted += scheduledList.size();

    }

    /**
     * Process a cloudlet (job) return event.
     *
     * @param ev a SimEvent object
     * @pre ev != $null
     * @post $none
     */
    @Override
    protected void processCloudletReturn(SimEvent ev) {
        Cloudlet cloudlet = (Cloudlet) ev.getData();

        Job job = (Job) cloudlet;

        /**
         * Generate a failure if failure rate is not zeros.
         */
        FailureGenerator.generate(job);

        getCloudletReceivedList().add(cloudlet);
        getCloudletSubmittedList().remove(cloudlet);

        CondorVM vm = (CondorVM) getVmsCreatedList().get(cloudlet.getVmId());
        //so that this resource is released
        vm.setState(WorkflowSimTags.VM_STATUS_IDLE);

        double delay = 0.0;
        if(Parameters.getOverheadParams().getPostDelay()!=null){
            delay = Parameters.getOverheadParams().getPostDelay(job);
        }
        schedule(this.workflowEngineId, delay, CloudSimTags.CLOUDLET_RETURN, cloudlet);

        cloudletsSubmitted--;
        //not really update right now, should wait 1 s until many jobs have returned

        schedule(this.getId(), 0.0, WorkflowSimTags.CLOUDLET_UPDATE);

    }

    /**
     * process cloudlet (job) check (not supported yet)
     *
     * @param ev a simEvent object
     */
    protected void processCloudletCheck(SimEvent ev) {
        /**
         * Left for future use.
         */
    }

    /**
     * bookkeeping of retry jobs to enable exceeded Storage checks for retried jobs (these have a new id after retry)
     */
    DisjointSetUnion retriedJobIdsSetUnion;

    public void setRetriedJobIdsSetUnion(DisjointSetUnion dsu) {
        retriedJobIdsSetUnion = dsu;
    }



    /**
     * for cloudlets stores storages where the cloudlet (when registering its output files) exceeded the available storage.
     * maps from cloudletId to set of datacenterIds
     * only applicable in federated partitioning context
     */
    Map<Integer, Set<Integer>> exceededStorageMap = new HashMap<Integer, Set<Integer>>();

    /**
     * process the event that a cloudlet finished, but the output files exceeded the site's storage capacity
     * only applicable in federated partitioning context
     *
     * @param ev a simEvent object
     */
    protected void processCloudletOutputExceededStorage(SimEvent ev) {
        // get cloudlet/job
        Cloudlet cloudlet = (Cloudlet) ev.getData();
        Job job = (Job) ev.getData();

        Log.printLine("Job " + job.getTaskList().get(0).getType() + " exceeded output storage, resubmitting ");

        // get ID's of datacenter with exceeded storage and the cloudlet
        int senderId   = ev.getSource();
        int cloudletId = cloudlet.getCloudletId();

        // if this is a retry, we need to get the first id of this job
        // this work because of the way we store job Ids
        // the first occurrence of a (multiple times) retried job will always be the representative
        if (retriedJobIdsSetUnion.contains(cloudletId)) {
            cloudletId = retriedJobIdsSetUnion.find(cloudletId);
        }

        // bookkeeping for exceeded storages
        exceededStorageMap.computeIfAbsent(cloudletId, cl -> new HashSet<>()).add(senderId);

        try {
            job.setCloudletStatus(Cloudlet.FAILED);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        processCloudletReturn(ev);



        // old resubmit below, was not working because cloudsim does not support resubmitting the same job again
        /*

        // add cloudlet to list of tasks again
        getCloudletList().add(cloudlet);

        // remove from submitted list
        boolean result = getCloudletSubmittedList().remove(cloudlet);
        if (!result){
            throw new RuntimeException("cloudlet should be resubmitted but was not present in the submitted list" );

        }

        // reset submitted counter
        cloudletsSubmitted -= 1;

        // reset job
        job.setTaskFinishTime(-1);
        job.setExecStartTime(-1);
        try {
            job.setCloudletStatus(Cloudlet.CREATED);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // sent cloudlet update to re-invoke scheduling
        schedule(this.getId(), 0.0, WorkflowSimTags.CLOUDLET_UPDATE);*/
    }

    /**
     * Start this entity (WorkflowScheduler)
     */
    @Override
    public void startEntity() {
        Log.printLine(getName() + " is starting...");
        // this resource should register to regional GIS.
        // However, if not specified, then register to system GIS (the
        // default CloudInformationService) entity.
        //int gisID = CloudSim.getEntityId(regionalCisName);
        int gisID = -1;
        if (gisID == -1) {
            gisID = CloudSim.getCloudInfoServiceEntityId();
        }

        // send the registration to GIS
        sendNow(gisID, CloudSimTags.REGISTER_RESOURCE, getId());
        //the below sentence is executed in workflow engine
        //schedule(getId(), 0, CloudSimTags.RESOURCE_CHARACTERISTICS_REQUEST);

    }

    /**
     * Terminate this entity (WorkflowScheduler)
     */
    @Override
    public void shutdownEntity() {

        clearDatacenters();
        Log.printLine(getName() + " is shutting down...");

    }

    /**
     * Submit cloudlets (jobs) to the created VMs. Scheduling is here
     *
     * @pre $none
     * @post $none
     */
    @Override
    protected void submitCloudlets() {

        sendNow(this.workflowEngineId, CloudSimTags.CLOUDLET_SUBMIT, null);
    }
    /**
     * A trick here. Assure that we just submit it once
     */
    private boolean processCloudletSubmitHasShown = false;

    /**
     * Submits cloudlet (job) list
     *
     * @param ev a simEvent object
     */
    protected void processCloudletSubmit(SimEvent ev) {
        List<Job> list = (List) ev.getData();
        getCloudletList().addAll(list);

        sendNow(this.getId(), WorkflowSimTags.CLOUDLET_UPDATE);
        if (!processCloudletSubmitHasShown) {
            //Log.printLine("Pay Attention that the actual vm size is " + getVmsCreatedList().size());
            processCloudletSubmitHasShown = true;
        }
    }

    /**
     * Process a request for the characteristics of a PowerDatacenter.
     *
     * @param ev a SimEvent object
     * @pre ev != $null
     * @post $none
     */
    @Override
    protected void processResourceCharacteristicsRequest(SimEvent ev) {

        setDatacenterCharacteristicsList(new HashMap<Integer, DatacenterCharacteristics>());

        Log.printLine(CloudSim.clock() + ": " + getName() + ": Cloud Resource List received with "
                + getDatacenterIdsList().size() + " resource(s)");

        for (Integer datacenterId : getDatacenterIdsList()) {
            sendNow(datacenterId, CloudSimTags.RESOURCE_CHARACTERISTICS, getId());
        }
    }
}
