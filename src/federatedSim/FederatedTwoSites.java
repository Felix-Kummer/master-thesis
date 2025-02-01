package federatedSim;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.*;

import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;
import org.workflowsim.*;
import org.workflowsim.utils.ClusteringParameters;
import org.workflowsim.utils.OverheadParameters;
import org.workflowsim.utils.Parameters;
import org.workflowsim.utils.ReplicaCatalog;

public class FederatedTwoSites {

	static int NUM_SITES; // number of sites

	//static double BW = 1.5e7;
	static double BW = 0;
	
	 // Partitioning Thresholds
    static int TASK_THRESHOLD;
    static int SEC_THRESHOLD;
    static double THRESHOLD_CHECKING_INTERVAL = 1; // in which interval the thresholds are checked

	private static ConfigParser configParser = new ConfigParser();


	public static void main(String[] args) {

		// parse config File
		if (args.length < 1) {
			throw new RuntimeException("Need a parameter config file");
		}

		if (args.length >= 2) {
			try {
				FileOutputStream logfile = new FileOutputStream(args[1]);
				Log.setOutput(logfile);
			} catch (FileNotFoundException e) {
				throw new RuntimeException(e);
			}
		}

		String resultsPath = null;
		if (args.length >= 3) {
			resultsPath = args[2];
		}




		configParser.parse(args[0]);

		try {
			
			// Load workflow
			 //String daxPath = "/home/fk/Schreibtisch/master/workflowSim_git/config/dax/Montage_100.xml";
			 String daxPath = configParser.getWorkflowPath();
	            if(daxPath == null){
	                Log.printLine("[DONE]  Warning: Please replace daxPath with the physical path in your working environment!");
	                return;
	            }
	            File daxFile = new File(daxPath);
	            if(!daxFile.exists()){
	                Log.printLine("Warning: Please replace daxPath with the physical path in your working environment!");
	                return;
	            }

				NUM_SITES      = configParser.getNumSites();
				TASK_THRESHOLD = configParser.getTaskThreshold();
				SEC_THRESHOLD  = configParser.getSecThreshold();
	            

				// set strategy, PART = new approach, RND = random
				Parameters.SchedulingAlgorithm sch_method;
				if (configParser.getStrategy().equals("PART")) {
					sch_method = Parameters.SchedulingAlgorithm.DYNAMIC_PART;
				} else if (configParser.getStrategy().equals("RND")) {
					sch_method = Parameters.SchedulingAlgorithm.DYNAMIC_RND;
				} else {
					throw new RuntimeException("Named invalid strategy in config file, use RND or PART");
				}

				// we dynamically schedule, thus no planner
	            Parameters.PlanningAlgorithm pln_method = Parameters.PlanningAlgorithm.INVALID; 
	            

	            ReplicaCatalog.FileSystem file_system = ReplicaCatalog.FileSystem.SHARED; 
	            

				// No overheads
	            OverheadParameters op = new OverheadParameters(0, null, null, null, null, 0);;
	            
	            // No Clustering
	            ClusteringParameters.ClusteringMethod method = ClusteringParameters.ClusteringMethod.NONE;
	            ClusteringParameters cp = new ClusteringParameters(0, 0, method, null);
	            

				// Initialize static parameters
	            Parameters.init(NUM_SITES, daxPath, null,
	                    null, op, cp, sch_method, pln_method,
	                    null, 0);
	            ReplicaCatalog.init(file_system);

	            
	            
	            // enable thresholds for scheduling
	            Parameters.enable_thresholds(); 
	            Parameters.setTASK_THRESHOLD(TASK_THRESHOLD);
	            Parameters.setSEC_THRESHOLD(SEC_THRESHOLD);
	            Parameters.setTHRESHOLD_CHECKING_INTERVAL(THRESHOLD_CHECKING_INTERVAL);
	            
	            // before creating any entities.
	            int num_user = 1;   // number of grid users
	            Calendar calendar = Calendar.getInstance(); 
	            boolean trace_flag = false;  // mean trace events 
	            
	            // Initialize the CloudSim library
	            CloudSim.init(num_user, calendar, trace_flag);

				// Create a WorkflowPlanner with one scheduler.
				WorkflowPlanner wfPlanner = new WorkflowPlanner("planner_0", 1);

				// Create a WorkflowEngine
				WorkflowEngine wfEngine = wfPlanner.getWorkflowEngine();

				// create sites
	            List<WorkflowDatacenter> workflowDatacenters = new LinkedList<>();
				List<CondorVM> vmList = new LinkedList<>();
				createDatacenter(vmList, workflowDatacenters, wfEngine.getSchedulerId(0));

				// verify site creation
				if (vmList.size() != workflowDatacenters.size() || vmList.size() != NUM_SITES) {
					throw new Exception("An inconsistent amount of sites and vms was created");
				}

	            /**
	             * Submits this list of vms to this WorkflowEngine.
	             */
	            wfEngine.submitVmList(vmList);
	            
	            /**
	             * Binds the data centers with the scheduler.
	             */
	            for (int i=0; i<NUM_SITES; i++) {
	            	wfEngine.bindSchedulerDatacenter(workflowDatacenters.get(i).getId(),0);
	            }


	            CloudSim.startSimulation();


	            List<Job> outputList0 = wfEngine.getJobsReceivedList();


	            CloudSim.stopSimulation();
	            
	            printJobList(outputList0);

				writeResults(resultsPath, outputList0);


				Log.printLine("END");
	            
		} catch (Exception e) {
			e.printStackTrace();
            Log.printLine("The simulation has been terminated due to an unexpected error");
        }
	}

	protected static void createDatacenter(List<CondorVM> vmList, List<WorkflowDatacenter> datacenterList, int userId) {

		Map<Integer, ConfigParser.Site> sites = configParser.getDatacentersSpecs();
		// count datacenter ids
		for ( Integer id : sites.keySet() ) {
			ConfigParser.Site site = sites.get(id);

			String name = "Datacenter_" + id;

			// We need to create a list to store one or more Machines
			List<Host> hostList = new ArrayList<Host>();

			// some reused properties
			// Pe = processing unit
			int pes        = site.getPes();
			int mips       = site.getMips(); // mips per pe
			int hostId     = 0;
			int ram        = site.getRam();
			double storage = site.getStorage();
			int bw         = (int) Math.min(site.getIntraBw(), Integer.MAX_VALUE);

			List<Pe> peList = new ArrayList<Pe>();
			for (int i=0;i<pes;i++) {
				peList.add(new Pe(i, new PeProvisionerSimple(mips)));
			}

			// host size, this is not relevant to us, we just need to make sure that cloudsim can assigns a single VM to the host
			// thus we only need to set host storage = vm storage
			long hostStorage = 100000000;

			// create host = physical machine hosting VMs
			// we need one host per datacenter only
			hostList.add(
					new Host(
							hostId,
							new RamProvisionerSimple(ram),
							new BwProvisionerSimple(bw),
							hostStorage,
							peList,
							new VmSchedulerTimeShared(peList)));

			// Create a DatacenterCharacteristics object that stores the
			// properties of a data center: architecture, OS, list of
			// Machines, allocation policy: time- or space-shared, time zone
			// and its price (G$/Pe time unit).
			String arch = "x86";      // system architecture
			String os = "Linux";          // operating system
			String vmm = "Xen";
			double time_zone = 10.0;         // time zone this resource located


			// we don't care about monetary cost models for executing, we just use the values from examples/org/workflowsim/examples/WorkflowSimBasicExample1.java
			double cost = 3.0;              // the cost of using processing in this resource
			double costPerMem = 0.05;		// the cost of using memory in this resource
			double costPerStorage = 0.1;	// the cost of using storage in this resource
			double costPerBw = 0.1;			// the cost of using bw in this resource


			WorkflowDatacenter datacenter = null;
			DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
					arch, os, vmm, hostList, time_zone, cost, costPerMem, costPerStorage, costPerBw);


			LinkedList<Storage> storageList = new LinkedList<Storage>();

			double toSourceBandwidth = site.getIntraBw(); // to 'source' entity

			double intraBandwidth = site.getIntraBw(); // proxy value for intra site communication


			// IMPORTANT keep name as it is here so that storages have the same name as their datacenters
			ClusterStorage s1 = null;
			try {
				s1 = new ClusterStorage(name, storage);
			} catch (ParameterException e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			}

			// add connections and their bandwidth
			Map<Integer, Double> connections = site.getConnectionMap();

			for (Map.Entry<Integer, Double> connection : connections.entrySet()) {
				String otherSite = "Datacenter_" + connection.getKey(); // this exploits the common naming scheme
				double bandwidth = connection.getValue();
				s1.setBandwidth(otherSite, bandwidth);
			}

			// The bandwidth within a data center
			s1.setBandwidth("local", intraBandwidth);
			// The bandwidth to the source site
			s1.setBandwidth("source", toSourceBandwidth);

			// add files to storage
			for (ConfigParser.File file: site.getFiles()) {
				ReplicaCatalog.addStorageList(file.getName(), s1.getName());
				try {
					s1.addFile(new org.cloudbus.cloudsim.File(file.getName(), file.getSize()));
				} catch (RuntimeException | ParameterException e) {
					e.printStackTrace();
					throw new RuntimeException("cannot fit initial data distribution into sites");
				}
			}

			// add to storage list (will have only one element)
			storageList.add(s1);



			// create datacenter
			try {
				datacenter = new WorkflowDatacenter(name, characteristics, new VmAllocationPolicySimple(hostList), storageList, 0);
			} catch (Exception e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			}
			datacenterList.add(datacenter);


			// ### create VM ###
			//VM Parameters (same as site -> one VM per site)
			CondorVM vm = new CondorVM(id, userId, mips, pes,ram , bw, hostStorage, vmm, new CloudletSchedulerTimeShared());
			vmList.add(vm);
		}


	}

	
	/**
     * Prints the job objects
     *
     * @param list list of jobs
     */
    protected static void printJobList(List<Job> list) {
        int size = list.size();
        Job job;

        String indent = "    ";
        Log.printLine();
        Log.printLine("========== OUTPUT ==========");
        Log.printLine("Cloudlet ID" + indent + "NAME" + indent + "STATUS" + indent
                + "Data center ID" + indent + "VM ID" + indent + indent + "Time" + indent + "Start Time" + indent + "Finish Time" + indent + "Depth");

        DecimalFormat dft = new DecimalFormat("###.##");
        for (int i = 0; i < size; i++) {
            job = list.get(i);
            Log.print(indent + job.getCloudletId() + indent + indent);

			if (job.getClassType() == Parameters.ClassType.STAGE_IN.value) {
				Log.print("STAGE_IN" + indent);
			} else {
				Log.print(job.getTaskList().get(0).getType() + indent);
			}

            if (job.getCloudletStatus() == Cloudlet.SUCCESS) {
                Log.print("SUCCESS");

                Log.printLine(indent + indent + job.getResourceId() + indent + indent + indent + job.getVmId()
                        + indent + indent + indent + dft.format(job.getActualCPUTime())
                        + indent + indent + dft.format(job.getExecStartTime()) + indent + indent + indent
                        + dft.format(job.getFinishTime()) + indent + indent + indent + job.getDepth());
            } else if (job.getCloudletStatus() == Cloudlet.FAILED) {
                Log.print("FAILED");

                Log.printLine(indent + indent + job.getResourceId() + indent + indent + indent + job.getVmId()
                        + indent + indent + indent + dft.format(job.getActualCPUTime())
                        + indent + indent + dft.format(job.getExecStartTime()) + indent + indent + indent
                        + dft.format(job.getFinishTime()) + indent + indent + indent + job.getDepth());
            }
        }

    }

	private static void writeResults(String resultsPath, List<Job> jobs){
		double startTime = jobs.get(0).getExecStartTime();
		double finishTime = jobs.get(jobs.size() - 1).getFinishTime();
		double makespan = finishTime-startTime;

		if (resultsPath == null) {
			Log.printLine("Results:");
			Log.printLine("Data transferred between sites:" + Parameters.getTotalTransferredData());
			Log.printLine("Time for transferring data between sites:" + Parameters.getTotalDataTransferTime());
			Log.printLine("Makespan:" + makespan);
		}
		else {
			try (PrintWriter writer = new PrintWriter(resultsPath)) {
				writer.println("TransferredData," + Parameters.getTotalTransferredData());
				writer.println("TransferTime," + Parameters.getTotalDataTransferTime());
				writer.println("Makespan," + makespan);
			} catch (Exception e) {
				e.printStackTrace();
			}

		}

	}
}