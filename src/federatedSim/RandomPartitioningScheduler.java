package federatedSim;

import java.nio.file.Files;
import java.time.chrono.HijrahChronology;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.File;
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
	private int count = 1;
	
	Random rndRandom = new Random();
	
	@Override
	public void run() throws Exception {
		List<Cloudlet> cloudletList = getCloudletList();
		List<CondorVM> vms = getVmList();
		
		
		// TODO storage check
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
			List<CondorVM> fittingVms = new ArrayList();
			for (CondorVM condorVM : vms) {
				WorkflowDatacenter datacenter = (WorkflowDatacenter) condorVM.getHost().getDatacenter(); // has to be a WorkflowDatacenter
				ClusterStorage storage = (ClusterStorage) datacenter.getStorageList().get(0); // there should only be one storage, we check that within WorkflowDatacenter
				if (storage.getAvailableSpace() > totalInputSize) {
					fittingVms.add(condorVM);
				}	
			}
			
			
			// get random VM 
			CondorVM vm = fittingVms.get(rndRandom.nextInt(vms.size()));
			
			
			vm.setState(WorkflowSimTags.VM_STATUS_BUSY);
	        cloudlet.setVmId(vm.getId());
	        getScheduledList().add(cloudlet);
		}

	}

}
