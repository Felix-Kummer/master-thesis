package federatedSim;

import federatedSim.utils.DisjointSetUnion;
import org.cloudbus.cloudsim.File;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSim;
import org.workflowsim.*;
import org.workflowsim.scheduling.BaseSchedulingAlgorithm;
import org.workflowsim.utils.Parameters;
import org.workflowsim.utils.ReplicaCatalog;

import javax.management.DynamicMBean;
import java.util.*;

public class PartitioningScheduler extends BaseSchedulingAlgorithm {

	private Map<Integer, Set<Integer>> exceededMap = new HashMap<>();
	public void addExceededMap(Map<Integer, Set<Integer>> map) {
		exceededMap = map;
	}

	// retried job id bookkeeping
	private DisjointSetUnion retriedJobsSetUnion;

	public void addRetriedJobsSetUnion (DisjointSetUnion dsu) {
		retriedJobsSetUnion = dsu;
	}

	private final int vmNum = Parameters.getVmNum();


	// available vms
	private List<CondorVM> vms;

	// datacenters of VMs
	private WorkflowDatacenter[] datacenters;

	// ids of datacenters
	private int[] datacenterIds;

	// storages of datacenters
	private ClusterStorage[] storages;

	// available storage capacity;
	private double[] availableStorage;

	public PartitioningScheduler() {

		// available vms (= sites)
		vms = getVmList();

		// sort vms so that first ID is at first index and so on
		vms.sort(Comparator.comparingInt(CondorVM::getId));

		// get datacenters, storages and cpacaities for each site
		datacenters      = new WorkflowDatacenter[vms.size()];
		datacenterIds    = new int[vms.size()];
		storages         = new ClusterStorage[vms.size()];
		availableStorage = new double[vms.size()];


		for (int i = 0; i < vms.size(); i++){
			datacenters[i]      = (WorkflowDatacenter) vms.get(i).getHost().getDatacenter();
			datacenterIds[i]    = datacenters[i].getId();
			storages[i]         = (ClusterStorage) datacenters[i].getStorageList().get(0);
			availableStorage[i] = storages[i].getAvailableSpace();
		}

	}

	// differences between available storage and predicted site assignment
	private double[] predictedAvailableStorage;




	@Override
	public void run() throws Exception {


		// ### SETUP ###


		// jobs that we should schedule
		List<Job> jobList = getCloudletList();


		// ### INITIAL PARTITIONING ###


		// all start jobs are in the first batch and later batches cannot contain start jobs
		// -> if one job has no parents -> this is the initial batch
		if (jobList.get(0).getParentList().size() == 0) {

			// compute initial partitioning
			initialPartitioning(jobList);

			// initialize dag with initial partitioning
			DynamicAbstractDag.processInitialPartitioning(jobList);

			// we're done with the first partitioning
			return;
		}



		// ### UPDATE DAG STRUCTURE ###


		// update dynamic DAG
		DynamicAbstractDag.updateDynamicInformation(jobList, getReceivedList());


		// ### SCHEDULING (PARTITIONING) ###


		// get current total estimated site mapping sizes
		double[] currentTotalSizeEstimates = DynamicAbstractDag.getTotalSizeEstimates();

		// determine which sites are predicted to overuse their storage, available storage - estimated size
		// negative: storage overuse predicted
		predictedAvailableStorage = new double[vmNum];
		for (int i = 0; i < vmNum; i++){
			predictedAvailableStorage[i] = availableStorage[i] - currentTotalSizeEstimates[i];
		}


		// iterate new jobs that should be scheduled
		for (Job job : jobList) {


			// get best site continuation for job
			int continuationSiteId = deriveBestSiteContinuation(job);

			// check if this is a retriedJob
			int originalId = -1;
			boolean retried = false;
			Set<Integer> exceededDatacenterIds = new HashSet<>();
			if (retriedJobsSetUnion.contains(job.getCloudletId())) {
				originalId = retriedJobsSetUnion.find(job.getCloudletId());
				retried = true;
				exceededDatacenterIds = exceededMap.get(originalId);
			}

			// if this job is retried and the job has previously exceeded its continuation site
			if (retried && exceededDatacenterIds.contains(datacenterIds[continuationSiteId])) {
				// we need to move the job to a different site anyway
				moveJob(job, continuationSiteId, exceededDatacenterIds); // ID here signals the site to exclude
				continue;
			}

			// check if job's best continuation is in sites with storage overuse
			// If that's the case, we need to move it (or its childs) = repartition
			if (predictedAvailableStorage[continuationSiteId] < 0 ) {

				// get real input size (we know all input files for job now)
				double realInputSize = 0;
				for (Object o : job.getFileList()) {
					File inputFile = (File) o;
					if (inputFile.getType() == Parameters.FileType.INPUT.value) {
						realInputSize += inputFile.getSize();
					}
				}

				// compute full size = real input size + estimated output size
				double newJobSize = realInputSize + DynamicAbstractDag.getEstimatedOutputSize(job);

				// check if we need to move the job right now (estimated size > capacity left)
				if (newJobSize > Math.abs(predictedAvailableStorage[continuationSiteId])) { // the value if this array has to be negative, see parent if condition
					moveJob(job, continuationSiteId, exceededDatacenterIds);

				// job's subtree fits into best site continuation, we now have to decide if we move job,
				// or forward the decision to job's children in later scheduling iteration
				} else {

					// remember whether we should move job now
					boolean moveJob = true;

					// iterate children
					for (Task childrenTask : job.getChildList()) {
						Job children = (Job) childrenTask;

						// check if we can move children instead of current job to reduce the amount of data transfers and delay for better estimates
						if (DynamicAbstractDag.getEstimatedInputSize(children) < realInputSize) {

							// moving children may be beneficiary -> leave job on its continuation site and delay move decision
							moveJob = false;
							break;
						}

					}
					// move job ?
					if (moveJob) {
						moveJob(job, continuationSiteId, exceededDatacenterIds); // the second argument defines where not to move the job

					} else {
						// schedule job to it's continuation
						scheduleJob(job, continuationSiteId);
					}

				}

			// job can be scheduled into its best continuation
			} else {
				scheduleJob(job, continuationSiteId);
			}
		}

	}


	// computes the best continuation of partitioning
	// this is the site where the smallest amount of data needs to be transferred based on parents
	// returns the vmID of the best site
	private int deriveBestSiteContinuation(Job job) {
		// store the amount of data that would be transferred for each site
		long[] requiredTransfers = new long[vmNum];

		// iterate input files
		for (Object o : job.getFileList()) {
			File file = (File) o;

			if (file.getType() == Parameters.FileType.INPUT.value) {
				continue;
			}

			// name of the input file (unique)
			String inputName = file.getName();

			// iterate parents
			for (Object parent : job.getParentList()) {
				Job parentJob = (Job) parent;

				// iterate parents output file
				for (Object o2 : parentJob.getFileList()) {
					File parentFile = (File) o2;

					// check if parents output name matches child's input name
					if (parentFile.getType() == Parameters.FileType.OUTPUT.value && parentFile.getName().equals(inputName)) {

						// add file size to sites different from the parent's site (we would need to transfer the file to the other site if we put the job there)
						int parentId = parentJob.getVmId();
						int fileSize = parentFile.getSize();
						for (int i = 0; i < vmNum; i++) {
							if (i == parentId) {
								continue;
							} else {
								requiredTransfers[i] += fileSize;
							}
						}
					}
				}
			}
		}

		// return siteId (vmID) with min transfer costs
		long minVal = 0;
		int minID   = 0;
		for (int i = 0; i < requiredTransfers.length ; i++){
			if (requiredTransfers[i] < minVal){
				minVal = requiredTransfers[i];
				minID = i;
			}
		}

		return minID;
	}

	// schedule job to vm with vmId id
	private void scheduleJob(Job job, int id) {
		vms.get(id).setState(WorkflowSimTags.VM_STATUS_BUSY);
		job.setVmId(id);
		getScheduledList().add(job);
	}

	// find the best site for a job, except continuationSite, and schedule it there
	// best site = has enough space for the predicted subtree and highest bandwidth from continuation site
	// exceededDatacenterIds contains the Ids of datacenter that prior repetitions of this job have exceeded
	private void moveJob(Job job, int continuationSiteId, Set<Integer> exceededDatacenterIds ) {

		// get size of job's subtree
		double parentSubtreeSize = DynamicAbstractDag.getEstimatedSize4SubDAG(job);

		// compute sites that can fit the (predicted) subtree of job
		double bestBandwidth = 0.0; // remember best bandwidth
		int siteId = -1; // remember site with the best bandwidth that has the most storage
		for (int i = 0; i < vmNum; i++) {
			// check if site can fit job's subtree and if the bandwidth is better than current best
			if (i != continuationSiteId && 								  // check if site is continuation -> skip site
					!exceededDatacenterIds.contains(datacenterIds[i]) &&  // check if job has previously exceeded site -> skip site
					predictedAvailableStorage[i] > 0 &&					  // check if site is predicted to have storage
					parentSubtreeSize < predictedAvailableStorage[i] ) {  // check if the site is predicted to have enough storage

				// we need the name of the datacenter because that's how cluster storage remembers bandwidth
				String targetSiteName = datacenters[i].getName();

				// get bandwidth from continuation site to target site
				double bandwidth = storages[continuationSiteId].getMaxTransferRate(targetSiteName);

				// compare bandwidth with current best
				if (bandwidth > bestBandwidth) {
					bestBandwidth = bandwidth;
					siteId = i;
				}

			}
		}

		// if we could not find a site, we just use the site that has the largest available storage (including continuation)
		// we also allow negative (predicted overuse here)
		// this is caused by either generally not enough storage, or immature estimates
		// if no space is available nowhere, we log and continue with the continuation
		if (siteId == -1) {
			double bestAvailableStorage = -1;
			for (int i = 0; i < vmNum; i++ ){
				if (predictedAvailableStorage[i] > bestAvailableStorage &&     // more space than previous candidate?
						!exceededDatacenterIds.contains(datacenterIds[i])) {   // still can't reschedule to previously exceed storage
					bestAvailableStorage = predictedAvailableStorage[i];
					siteId = i;
				}
			}
		}

		// all sites were previously exceeded -> throw Failure to prevent infinite loop for this job
		if (siteId == -1) {
			throw new RuntimeException("Job " + job.getTaskList().get(0).getType() + " has exceeded all sides storages and cannot be scheduled, halting simulation");
		}

		// move job to new site
		scheduleJob(job, siteId);


		// update predicted available storage so that other Jobs can react to the expected changes in storage availability
		if (siteId == continuationSiteId) { return; } // no changes needed, task stay on continuation site
		predictedAvailableStorage[continuationSiteId] += parentSubtreeSize;
		predictedAvailableStorage[siteId]             -= parentSubtreeSize;

		// update dynamic DAG's shift array to reflect changes that violate the best continuation
		// increase for new siteId and decrease for continuationSiteId, the latter was predicted by the dynamic DAG and thus needs to be adapted
		DynamicAbstractDag.updateShiftArray(job, siteId, continuationSiteId);
	}

	private void initialPartitioning(List<Job> startJobs) {
		for (Job startJob : startJobs) {

			// keep track of sizes of input data on sites
			double[] sizeOnSites = new double[vmNum];

			for (Object o: startJob.getFileList()) {
				File file = (File) o;
				if (file.getType() == Parameters.FileType.INPUT.value) {

					// get list of storages that contain the file
					List<String> storageList = ReplicaCatalog.getStorageList(file.getName()); // this contains the names of the storage (we name storages like their datacenters)

					// this is the initial partitioning, file should thus only be present at one site
					if (storageList.size() != 1) {
						throw new RuntimeException("ERROR: A file was present at multiple sites after initial stage in to sites, or the file was not correctly stage into a site");
					}
					String datacenterName = CloudSim.getEntity(storageList.get(0)).getName();

					// get index of this datacenter in our datacenter arrays
					int index = -1;
					for (int i = 0; i < vmNum; i++){
						if (datacenters[i].getName().equals(datacenterName)) {
							index = i;
							break;
						}
					}

					// update size of input data on site
					sizeOnSites[index] += file.getSize();

				}
			}

			// determine which site has the most input data of the job, and which site has the most storage available
			double largestSize = -1;
			int bestSiteId     = -1;

			double mostStorage = -1;
			int mostStorageId  = -1;
			for (int i = 0; i< vmNum; i++ ) {
				if (sizeOnSites[i] > largestSize) {
					largestSize = sizeOnSites[i];
					bestSiteId = i;
				}
				if (availableStorage[i] > mostStorage){
					mostStorage = availableStorage[i];
					mostStorageId = i;
				}
			}

			//  no input data -> schedule to site with most storage
			if (bestSiteId == -1) {
				scheduleJob(startJob, mostStorageId);

			// otherwise, schedule to size where most input data resides
			} else {
				scheduleJob(startJob, bestSiteId);
			}
		}
	}
}
