package federatedSim;

import federatedSim.utils.DisjointSetUnion;
import org.cloudbus.cloudsim.File;
import org.cloudbus.cloudsim.core.CloudSim;
import org.workflowsim.*;
import org.workflowsim.scheduling.BaseSchedulingAlgorithm;
import org.workflowsim.utils.Parameters;
import org.workflowsim.utils.ReplicaCatalog;

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


	// differences between available storage and predicted site assignment
	private double[] predictedAvailableStorage;




	@Override
	public void run() throws Exception {


		// ### SETUP ###

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


		// jobs that we should schedule
		List<Job> jobList = getCloudletList();

		if (jobList.size() == 0) { // nothing to do
			return;
		}


		// ### INITIAL PARTITIONING ###


		// all start jobs are in the first batch
		// -> if all jobs have no parents, and the jobs are not retried -> this is the initial batch
		boolean startBatch = true;
		for (Job job : jobList) {

			// check if this is a start note
			if (job.getParentList().size() != 0) {
				startBatch = false;
				break;
			}

			// check if this is retried
			if (retriedJobsSetUnion.contains(job.getCloudletId())) {
				startBatch = false;
				break;
			}
		}

		// initial Partitioning
		if (startBatch) {

			// run sanity checks on sites to ensure that this scheduler can work properly
			sanityChecks();

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

			// special case: this is a retried start job
			if ( job.getParentList().size() == 0 ) {
				handleRetriedStartJob(job);
				continue;
			}


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

			if (file.getType() == Parameters.FileType.OUTPUT.value) {
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
		long minVal = Long.MAX_VALUE;
		int minID   = -1;
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
			int siteId = computeInitPartitioningSiteId(startJob);
			scheduleJob(startJob, siteId);
		}
	}

	// computes the site id for jobs of the initial (blind) partitioning
	private int computeInitPartitioningSiteId(Job startJob) {
		// keep track of sizes of input data on sites
		double[] sizeOnSites = new double[vmNum];

		for (Object o: startJob.getFileList()) {
			File file = (File) o;
			if (file.getType() == Parameters.FileType.INPUT.value) {

				// get list of storages that contain the file
				List<String> storageList = ReplicaCatalog.getStorageList(file.getName()); // this contains the names of the storage (we name storages like their datacenters)

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
			return mostStorageId;

			// otherwise, schedule to size where most input data resides
		} else {
			return bestSiteId;
		}
	}

	// derive a site Id for an retried start job
	private void handleRetriedStartJob(Job startJob) {

		// this should be a retried job
		if (!retriedJobsSetUnion.contains(startJob.getCloudletId())) {
			throw new RuntimeException("Start job " + startJob.getTaskList().get(0).getType() + " was not retried, but was submitted as a regular job in scheduling.");
		}


		// get initial site
		int previousVmId = startJob.getPreviousVmId();

		if (previousVmId == -1) {
			throw new RuntimeException("Retried start job does not contain its previous vm's id");
		}

		// get sites that this job has exceeded
		Integer originalId = retriedJobsSetUnion.find(startJob.getCloudletId());
		Set<Integer> exceededDatacenterIds = exceededMap.get(originalId);

		int bestId = -1;
		double bestStorage = Double.MIN_VALUE;
		for (int i = 0; i < vmNum; i++ ) {

			// site was exceeded, not an option
			if (exceededDatacenterIds.contains(datacenterIds[i])) {
				continue;
			}


			// check if this has better storage
			if (availableStorage[i] > bestStorage){
				bestId = i;
				bestStorage = availableStorage[i];
			}

		}

		// if no site was found -> error
		if (bestId == -1) {
			throw new RuntimeException("An initial job was not able to be executed on any site");
		}

		// update shift array for start node
		DynamicAbstractDag.updateShiftArray(startJob,bestId, previousVmId);

		// compute job size (we now input size and estimate outputs)
		double jobSize = DynamicAbstractDag.getEstimatedOutputSize(startJob);
		for (Object o : startJob.getFileList()) {
			File file = (File) o;
			if ( file.getType() == Parameters.FileType.INPUT.value) {
				jobSize += file.getSize();
			}
		}


		// update available storage
		predictedAvailableStorage[bestId]       += jobSize;
		predictedAvailableStorage[previousVmId] -= jobSize;

		// schedule Job
		scheduleJob(startJob, bestId);

	}

	private void sanityChecks() {
		// sanity checks on vm's and datacenters
		try {
			for (int j = 0; j < vms.size(); j++){
				WorkflowDatacenter workflowDatacenter = datacenters[j];
				if (!workflowDatacenter.getName().equals("Datacenter_" + j)) {
					throw new Exception("Datacenter has wrong name, that does not comply with its creation id");
				}
				if (workflowDatacenter.getVmList().size() != 1) {
					throw new Exception("Each site should have exactly one vm");
				}

				if (workflowDatacenter.getVmList().get(0).getId() != j) {
					throw new Exception("Wrong vm id in datacenter");
				}
			}

		} catch (Exception e) {
			throw new RuntimeException("Sanity checks after site creation failed, reason: " + e.getMessage());
		}
	}
}
