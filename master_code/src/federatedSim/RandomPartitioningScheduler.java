package federatedSim;

import java.nio.file.Files;
import java.time.chrono.HijrahChronology;
import java.util.*;

import federatedSim.utils.DisjointSetUnion;
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.File;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Storage;
import org.workflowsim.ClusterStorage;
import org.workflowsim.CondorVM;
import org.workflowsim.Job;
import org.workflowsim.WorkflowDatacenter;
import org.workflowsim.WorkflowSimTags;
import org.workflowsim.scheduling.BaseSchedulingAlgorithm;
import org.workflowsim.utils.Parameters;

import javafx.beans.value.WeakChangeListener;

public class RandomPartitioningScheduler extends BaseSchedulingAlgorithm {

	// maps each job (cloudlet) to the sites where it exceeded storage
	// this is stateful information and thus maintained in WorkflowScheduler
	private Map<Integer, Set<Integer>> exceededMap = new HashMap<>();
	public void addExceededMap(Map<Integer, Set<Integer>> map) {
		exceededMap = map;
	}

	// retried job id bookkeeping
	private DisjointSetUnion retriedJobsSetUnion;

	public void addRetriedJobsSetUnion (DisjointSetUnion dsu) {
		retriedJobsSetUnion = dsu;
	}

	Random rndRandom = new Random();
	
	@Override
	public void run() throws Exception {
		List<Cloudlet> cloudletList = getCloudletList();
		List<CondorVM> vms = getVmList();

		for (Cloudlet cloudlet : cloudletList) {
			
			// get files for the job (this includes output files)
			Job job = (Job) cloudlet;
			List<File> files = job.getFileList();
			
			// get input files and accumulate their sizes
			long totalInputSize  = 0L;
			for (File file : files) {
				if (file.getType() == Parameters.FileType.INPUT.value) {
					totalInputSize += file.getSize();
				}
			}
			
			// get VMs that have enough storage
			List<CondorVM> fittingVms = new ArrayList<>();
			for (CondorVM condorVM : vms) {
				WorkflowDatacenter datacenter = (WorkflowDatacenter) condorVM.getHost().getDatacenter(); // has to be a WorkflowDatacenter
				ClusterStorage storage = (ClusterStorage) datacenter.getStorageList().get(0); // there should only be one storage, we check that within WorkflowDatacenter

				// check if datacenters storage was previously exceeded by this job
				int datacenterId = datacenter.getId();
				int jobId        = cloudlet.getCloudletId();


				// if this is a retry, we need to get the first id of this job
				// this work because of the way we store job Ids
				// the first occurrence of a (multiple times) retried job will always be the representative
				if (retriedJobsSetUnion.contains(jobId)) {
					jobId = retriedJobsSetUnion.find(jobId);
				}


				// if (exceededMap.get(jobId).contains(datacenterId)){
				if (exceededMap.containsKey(jobId) && exceededMap.get(jobId).contains(datacenterId)){
					// skip this site (VM) because it previously exceeded the storage with its output sites
					continue;
				}

				// check for available capacity
				if (storage.getAvailableSpace() > totalInputSize) {
					fittingVms.add(condorVM);
				}	
			}

			if (fittingVms.size() < 1) {
				Log.printLine("No site has enough storage capacity to run the task - Aborting Simulation");
				throw new RuntimeException("No site has enough storage capacity to run the task");
			}

			// get random VM 
			CondorVM vm = fittingVms.get(rndRandom.nextInt(fittingVms.size()));


			// debug only
			// vm = vms.get(1);

			vm.setState(WorkflowSimTags.VM_STATUS_BUSY);
			cloudlet.setVmId(vm.getId());
			getScheduledList().add(cloudlet);
		}

	}

}
