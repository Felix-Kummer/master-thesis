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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import federatedSim.StorageException;
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.CloudletScheduler;
import org.cloudbus.cloudsim.Consts;
import org.cloudbus.cloudsim.Datacenter;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.File;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.VmAllocationPolicy;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;
import org.workflowsim.utils.ReplicaCatalog;
import org.workflowsim.utils.Parameters;
import org.workflowsim.utils.Parameters.ClassType;
import org.workflowsim.utils.Parameters.FileType;

/**
 * WorkflowDatacenter extends Datacenter so as we can use CondorVM and other
 * components
 *
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Apr 9, 2013
 */
@SuppressWarnings("ForLoopReplaceableByForEach")
public class WorkflowDatacenter extends Datacenter {

    public WorkflowDatacenter(String name,
            DatacenterCharacteristics characteristics,
            VmAllocationPolicy vmAllocationPolicy,
            List<Storage> storageList,
            double schedulingInterval) throws Exception {
        super(name, characteristics, vmAllocationPolicy, storageList, schedulingInterval);

    }

    @Override
    protected void processOtherEvent(SimEvent ev) {
    }

    /**
     * Processes a Cloudlet submission. The cloudlet is actually a job which can
     * be cast to org.workflowsim.Job
     *
     * @param ev a SimEvent object
     * @param ack an acknowledgement
     * @pre ev != null
     * @post $none
     */
    @Override
    protected void processCloudletSubmit(SimEvent ev, boolean ack) {

        updateCloudletProcessing();

        try {
            /**
             * cl is actually a job but it is not necessary to cast it to a job
             */
            Cloudlet cl = (Cloudlet) ev.getData();

            if (cl.isFinished()) {
                String name = CloudSim.getEntityName(cl.getUserId());
                Log.printLine(getName() + ": Warning - Cloudlet #" + cl.getCloudletId() + " owned by " + name
                        + " is already completed/finished.");
                Log.printLine("Therefore, it is not being executed again");
                Log.printLine();

                // NOTE: If a Cloudlet has finished, then it won't be processed.
                // So, if ack is required, this method sends back a result.
                // If ack is not required, this method don't send back a result.
                // Hence, this might cause CloudSim to be hanged since waiting
                // for this Cloudlet back.
                if (ack) {
                    int[] data = new int[3];
                    data[0] = getId();
                    data[1] = cl.getCloudletId();
                    data[2] = CloudSimTags.FALSE;

                    // unique tag = operation tag
                    int tag = CloudSimTags.CLOUDLET_SUBMIT_ACK;
                    sendNow(cl.getUserId(), tag, data);
                }

                sendNow(cl.getUserId(), CloudSimTags.CLOUDLET_RETURN, cl);

                return;
            }

            int userId = cl.getUserId();
            int vmId = cl.getVmId();
            Host host = getVmAllocationPolicy().getHost(vmId, userId);
            CondorVM vm = (CondorVM)host.getVm(vmId, userId);

            switch (Parameters.getCostModel()) {
                case DATACENTER:
                    // process this Cloudlet to this CloudResource
                    cl.setResourceParameter(getId(), getCharacteristics().getCostPerSecond(), 
                            getCharacteristics().getCostPerBw());
                    break;
                case VM:
                    cl.setResourceParameter(getId(), vm.getCost(), vm.getCostPerBW());
                    break;
                default:
                    break;
            }


            /**
             * Stage-in file && Shared based on the file.system
             */
            // we disable these jobs in favor of directly placing data in sites
            if (cl.getClassType() == ClassType.STAGE_IN.value) { 
                stageInFile2FileSystem(cl);
            }

            /**
             * Add data transfer time (communication cost
             */
            /**
             * Cast cl to job so as we can use its functions
             */
            Job job = (Job) cl;

            double fileTransferTime = 0.0;
            if (cl.getClassType() == ClassType.COMPUTE.value) {
                fileTransferTime = processDataStageIn(job.getFileList(), cl);
            }


            CloudletScheduler scheduler = vm.getCloudletScheduler();
            double estimatedFinishTime = scheduler.cloudletSubmit(cl, fileTransferTime);
            updateTaskExecTime(job, vm);

            // if this cloudlet is in the exec queue
            if (estimatedFinishTime > 0.0 && !Double.isInfinite(estimatedFinishTime)) {
                send(getId(), estimatedFinishTime, CloudSimTags.VM_DATACENTER_EVENT);
            } else {
                Log.printLine("Warning: You schedule cloudlet to a busy VM");
            }

            if (ack) {
                int[] data = new int[3];
                data[0] = getId();
                data[1] = cl.getCloudletId();
                data[2] = CloudSimTags.TRUE;

                int tag = CloudSimTags.CLOUDLET_SUBMIT_ACK;
                sendNow(cl.getUserId(), tag, data);
            }
        } catch (ClassCastException c) {
            Log.printLine(getName() + ".processCloudletSubmit(): " + "ClassCastException error.");

        } catch (Exception e) {
            Log.printLine(getName() + ".processCloudletSubmit(): " + "Exception error.");
            e.printStackTrace();

        }

        checkCloudletCompletion();
    }

    /**
     * Update the submission time/exec time of a task
     *
     * @param job
     * @param vm
     */
    private void updateTaskExecTime(Job job, Vm vm) {

        double start_time = job.getExecStartTime();
        for (Task task : job.getTaskList()) {
            task.setExecStartTime(start_time);
            double task_runtime = task.getCloudletLength() / vm.getMips();
            start_time += task_runtime;
            //Because CloudSim would not let us update end time here
            task.setTaskFinishTime(start_time);
        }

    }

    /**
     * Stage in files for a stage-in job. For a local file system (such as
     * condor-io) add files to the local storage; For a shared file system (such
     * as NFS) add files to the shared storage
     *
     * @param cl, the job
     * @pre $none
     * @post $none
     */
    private void stageInFile2FileSystem(Cloudlet cl) {
        Task t1 = (Task) cl;
        List fList = t1.getFileList();

        for (Iterator it = fList.iterator(); it.hasNext();) {
            org.cloudbus.cloudsim.File file = (org.cloudbus.cloudsim.File) it.next();

            switch (ReplicaCatalog.getFileSystem()) {
                /**
                 * For local file system, add it to local storage (data center
                 * name)
                 */
                case LOCAL:

                    ReplicaCatalog.addStorageList(file.getName(), this.getName());
                    /**
                     * Is it not really needed currently but it is left for
                     * future usage
                     */
                    //ClusterStorage storage = (ClusterStorage) getStorageList().get(0);
                    //storage.addFile(file);
                    break;
                /**
                 * For shared file system, add it to the shared storage
                 */
                case SHARED:
                    ReplicaCatalog.addStorageList(file.getName(), this.getName());
					ClusterStorage thisStorage;
					try {
						thisStorage = checkAndGetStorage(this);
						thisStorage.addFile(file);
					} catch (Exception e) {
						e.printStackTrace();
						System.exit(1);
					}
                    
                    break;
                
                default:
                    break;
            }

        }
    }

    /**
     * If a input file has an output file it does not need stage-in For
     * workflows, we have a rule that a file is written once and read many
     * times, thus if a file is an output file it means it is generated within
     * this job and then used by another job within the same job (or other jobs
     * maybe) This is useful when we perform horizontal clustering
     *
     * @param list, the list of all files
     * @param file, the file to be examined
     * @pre $none
     * @post $none
     */
    private boolean isRealInputFile(List<File> list, File file) {
        if (file.getType() == FileType.INPUT.value)//input file
        {
            for (File another : list) {
                if (another.getName().equals(file.getName())
                        /**
                         * if another file is output file
                         */
                        && another.getType() == FileType.OUTPUT.value) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }
    /*
     * Stage in for a single job (both stage-in job and compute job)
     * @param requiredFiles, all files to be stage-in
     * @param cl, the job to be processed
     * @pre  $none
     * @post $none
     */

    protected double processDataStageIn(List<File> requiredFiles, Cloudlet cl) throws Exception {
        double time = 0.0;
        Iterator<File> iter = requiredFiles.iterator();
        while (iter.hasNext()) {
            File file = iter.next();
            //The input file is not an output File 
            if (isRealInputFile(requiredFiles, file)) {
                double maxBwth = 0.0;
                List siteList = ReplicaCatalog.getStorageList(file.getName());
                if (siteList.isEmpty()) {
                    throw new Exception(file.getName() + " does not exist");
                }
                switch (ReplicaCatalog.getFileSystem()) {
                    case SHARED:
                        //stage-in job
                        /**
                         * Picks up the site that is closest
                         */
                        double maxRate = Double.MIN_VALUE;

                        // check if this site already contains the file
                        if (siteList.contains(this.getName())) {
                            ClusterStorage thisStorage = (ClusterStorage) this.getStorageList().get(0);
                            maxRate = thisStorage.getMaxTransferRate("local");
                            time += file.getSize() / maxRate;
                            break;
                        }


                        for (Iterator it = siteList.iterator(); it.hasNext();) {

                            
                        	String site = (String) it.next();
                        	
                        	// need to avoid the source object
                        	if (site.equals("source")) {
                        		continue;
                        	}
                        	
                        	// We exploit here that we name datacenter's cluster storages the same name as the datacenters
                        	WorkflowDatacenter datacenter = (WorkflowDatacenter) CloudSim.getEntity(site);
                        	
                        	// validate and get the cluster storage
                        	ClusterStorage storage = checkAndGetStorage(datacenter);
                        	
                        	// get transfer time from other datacenter to this (defined through ClusterStorages)
                        	double rate = storage.getMaxTransferRate(this.getName()); 
                            if (rate > maxRate) {
                                maxRate = rate;
                            }
                        }
                        // add the file to this datacenter's cluster storage 
                        ReplicaCatalog.addStorageList(file.getName(), getName());
                        ClusterStorage thisStorage = checkAndGetStorage(this);
                        thisStorage.addFile(file);
                        
                        time += file.getSize() / maxRate;

                        Parameters.setTotalTransferredData(Parameters.getTotalTransferredData() + file.getSize());
                        Parameters.setTotalDataTransferTime(Parameters.getTotalDataTransferTime() + time);

                        break;
                    case LOCAL:

                        int vmId = cl.getVmId();
                        int userId = cl.getUserId();
                        Host host = getVmAllocationPolicy().getHost(vmId, userId);
                        Vm vm = host.getVm(vmId, userId);

                        boolean requiredFileStagein = true;

                        for (Iterator it = siteList.iterator(); it.hasNext();) {
                            //site is where one replica of this data is located at
                            String site = (String) it.next();
                            if (site.equals(this.getName())) {
                                continue;
                            }
                            /**
                             * This file is already in the local vm and thus it
                             * is no need to transfer
                             */
                            if (site.equals(Integer.toString(vmId))) {
                                requiredFileStagein = false;
                                break;
                            }
                            double bwth;
                            if (site.equals(Parameters.SOURCE)) {
                                //transfers from the source to the VM is limited to the VM bw only
                                bwth = vm.getBw();
                                //bwth = dcStorage.getBaseBandwidth();
                            } else {
                                //transfers between two VMs is limited to both VMs
                                bwth = Math.min(vm.getBw(), getVmAllocationPolicy().getHost(Integer.parseInt(site), userId).getVm(Integer.parseInt(site), userId).getBw());
                                //bwth = dcStorage.getBandwidth(Integer.parseInt(site), vmId);
                            }
                            if (bwth > maxBwth) {
                                maxBwth = bwth;
                            }
                        }
                        if (requiredFileStagein && maxBwth > 0.0) {
                            time += file.getSize() / Consts.MILLION * 8 / maxBwth;
                        }

                        /**
                         * For the case when storage is too small it is not
                         * handled here
                         */
                        //We should add but since CondorVm has a small capability it often fails
                        //We currently don't use this storage to do anything meaningful. It is left for future. 
                        //condorVm.addLocalFile(file);
                        ReplicaCatalog.addStorageList(file.getName(), Integer.toString(vmId));
                        break;
                }
            }
        }
        return time;
    }

    @Override
    protected void updateCloudletProcessing() {
        // if some time passed since last processing
        // R: for term is to allow loop at simulation start. Otherwise, one initial
        // simulation step is skipped and schedulers are not properly initialized
        //this is a bug of CloudSim if the runtime is smaller than 0.1 (now is 0.01) it doesn't work at all
        if (CloudSim.clock() < 0.111 || CloudSim.clock() > getLastProcessTime() + 0.01) {
            List<? extends Host> list = getVmAllocationPolicy().getHostList();
            double smallerTime = Double.MAX_VALUE;
            // for each host...
            for (int i = 0; i < list.size(); i++) {
                Host host = list.get(i);
                // inform VMs to update processing
                double time = host.updateVmsProcessing(CloudSim.clock());
                // what time do we expect that the next cloudlet will finish?
                if (time < smallerTime) {
                    smallerTime = time;
                }
            }
            // gurantees a minimal interval before scheduling the event
            if (smallerTime < CloudSim.clock() + 0.11) {
                smallerTime = CloudSim.clock() + 0.11;
            }
            if (smallerTime != Double.MAX_VALUE) {
                schedule(getId(), (smallerTime - CloudSim.clock()), CloudSimTags.VM_DATACENTER_EVENT);
            }
            setLastProcessTime(CloudSim.clock());
        }
    }

    /**
     * Verifies if some cloudlet inside this PowerDatacenter already finished.
     * If yes, send it to the User/Broker
     *
     * @pre $none
     * @post $none
     */
    @Override
    protected void checkCloudletCompletion() {
        List<? extends Host> list = getVmAllocationPolicy().getHostList();
        for (int i = 0; i < list.size(); i++) {
            Host host = list.get(i);
            for (Vm vm : host.getVmList()) {
                while (vm.getCloudletScheduler().isFinishedCloudlets()) {
                    Cloudlet cl = vm.getCloudletScheduler().getNextFinishedCloudlet();
                    if (cl != null) {
                        // attempt to add output files to local cluster storage
                        try {
                            // register files to this datacenter's cluster storage
                            register(cl);

                            // no exception -> cloudlet finished successfully -> inform scheduler
                            sendNow(cl.getUserId(), CloudSimTags.CLOUDLET_RETURN, cl);

                        } catch (StorageException e) {
                            // cloudlet's output files exceeded the locally available storage
                            Log.printLine("Task's " +  cl.getCloudletId() + " output files have exceeded the capacity of "
                                + this.getName() + "'s storage.");


                            // inform scheduler about exceeded storage -> needs to resubmit to another datacenter
                            sendNow(cl.getUserId(), WorkflowSimTags.CLOUDLET_STORAGE_EXCEEDED, cl);
                        }
                    }
                }
            }
        }
    }
    /*
     * Register a file to the storage if it is an output file
     * @param requiredFiles, all files to be stage-in
     * @param cl, the job to be processed
     * @pre  $none
     * @post $none
     */

    private void register(Cloudlet cl) throws StorageException {
        Task tl = (Task) cl;
        List fList = tl.getFileList();

        switch (ReplicaCatalog.getFileSystem()) {
            case SHARED:
                // get this DataCenter's ClusterStorage
                ClusterStorage thisStorage = null;
                try {
                    thisStorage = checkAndGetStorage(this);
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(1);
                }

                // List of output files
                List<File> outputFileList = new ArrayList<>();

                // Bookkeeping for file output sizes
                double outputSize = 0.0;

                // iterate all files used by the task (includes inputs)
                for (Iterator it = fList.iterator(); it.hasNext();){
                    org.cloudbus.cloudsim.File file = (org.cloudbus.cloudsim.File) it.next();
                    if (file.getType() == FileType.OUTPUT.value) { // is output file -> need  to add to storage

                        // Add output file
                        outputFileList.add(file);

                        // Add size to total output size
                        outputSize += file.getSize();
                    }
                }

                // cannot fit all files into storage
                if (thisStorage.getAvailableSpace() < outputSize) {
                    // let parent call know with custom Exception that signals the output size
                    throw new StorageException(outputSize);

                // can fit all files -> add them
                } else {
                    // add files to storage
                    double storageResult = thisStorage.addFile(outputFileList);

                    // if all output files have been added, without errors, add them to the ReplicaCatalog so that they are findabale
                    for (File file : outputFileList) {
                        ReplicaCatalog.addStorageList(file.getName(), this.getName());
                    }
                }

                break;

            case LOCAL:
                for (Iterator it = fList.iterator(); it.hasNext();) {
                    org.cloudbus.cloudsim.File file = (org.cloudbus.cloudsim.File) it.next();
                    if (file.getType() == FileType.OUTPUT.value) { //output file
                        int vmId = cl.getVmId();
                        int userId = cl.getUserId();
                        Host host = getVmAllocationPolicy().getHost(vmId, userId);
                        /**
                         * Left here for future work
                         */
                        CondorVM vm = (CondorVM) host.getVm(vmId, userId);

                        ReplicaCatalog.addStorageList(file.getName(), Integer.toString(vmId));
                    }
                }

                break;
        }
    }
    
    /*
     * This methods is exclusively designed for the Federated Partition Aproach
     * It checks whether @param datacenter has exectly one storage of type Cluster Storage and returns it
     */
    static private ClusterStorage checkAndGetStorage(WorkflowDatacenter datacenter) throws Exception {
    	
    	// get the storage list
    	List<Storage> storageList = datacenter.getStorageList();
    	
    	// ensure that the datacenter has exactly one storage and that it is a cluster Storage
    	if (storageList.size() > 1) {
    		throw new Exception("Datacenters should have only one storage for Federated Partitioning Evaluation");
    	}
    	if (!(storageList.get(0) instanceof ClusterStorage)) {
    		throw new Exception("A datacenter has a invalid Storage type, "
    				+ "should be ClusterStorage for Federated Partitioning Evaluation.");
    	}
    	
    	ClusterStorage storage = (ClusterStorage) storageList.get(0);
    	return storage;
    	
    }
}
