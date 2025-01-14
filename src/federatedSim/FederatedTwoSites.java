package federatedSim;

import java.io.File;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;

import javax.sound.midi.Patch;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.CloudletSchedulerSpaceShared;
import org.cloudbus.cloudsim.CloudletSchedulerTimeShared;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.HarddriveStorage;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.VmAllocationPolicySimple;
import org.cloudbus.cloudsim.VmSchedulerTimeShared;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;
import org.cloudbus.cloudsim.NetworkTopology;
import org.workflowsim.ClusterStorage;
import org.workflowsim.CondorVM;
import org.workflowsim.WorkflowDatacenter;
import org.workflowsim.Job;
import org.workflowsim.WorkflowEngine;
import org.workflowsim.WorkflowPlanner;
import org.workflowsim.utils.ClusteringParameters;
import org.workflowsim.utils.OverheadParameters;
import org.workflowsim.utils.Parameters;
import org.workflowsim.utils.ReplicaCatalog;

public class FederatedTwoSites {
	
	// TODO site params
	static int NUM_SITES = 2; // 2 sites, 2 Vms
	
	
	// TODO maybe change these params!
	/*
	static int STORAGE = 1000000; // storage at sites
	static int RAM     = 2048;    // Ram at sites
	static int MIPS    = 1000;	  // Million instructions per second at sites
	static int PES     = 20;      // Number of processing units per site 
	*/
	
	static int STORAGE = 1000000; // storage at sites
	static int RAM     = 2048;    // Ram at sites
	static int MIPS    = 100;	  // Million instructions per second at sites
	static int PES     = 2;
	
	
	// TODO bandwidth
	
	
	 // Partitioning Thresholds //TODO change these
    
    static int TASK_THRESHOLD = 0;
    static int SEC_THRESHOLD  = 0;
    static double THRESHOLD_CHECKING_INTREVAL = 1; // in which interval the thresholds are checked
	
	
	protected static List<CondorVM> createVM(int userId, int vms) {

        //Creates a container to store VMs. This list is passed to the broker later
        LinkedList<CondorVM> list = new LinkedList<CondorVM>();

        //VM Parameters (same as site -> one VM per site) 
        long size = STORAGE; //image size (MB)
        int ram = RAM; //vm memory (MB)
        int mips = MIPS;
        long bw = 1000; // TODO
        int pesNumber = PES; //number of cpus
        String vmm = "Xen"; //VMM name

        //create VMs
        CondorVM[] vm = new CondorVM[vms];

        for (int i = 0; i < vms; i++) {
            double ratio = 1.0;
            vm[i] = new CondorVM(i, userId, mips * ratio, pesNumber, ram, bw, size, vmm, new CloudletSchedulerTimeShared()); 
            list.add(vm[i]);
        }

        return list;
    }
	
	public static void main(String[] args) {
		try {
			
			// Load workflow
			 //String daxPath = "/home/fk/Schreibtisch/master/workflowSim_git/config/dax/Montage_100.xml";
			 String daxPath = "/home/fk/Schreibtisch/master/workflowSim_git/config/dax/test_dax/two_task_memory.xml";
	            if(daxPath == null){
	                Log.printLine("[DONE]  Warning: Please replace daxPath with the physical path in your working environment!");
	                return;
	            }
	            File daxFile = new File(daxPath);
	            if(!daxFile.exists()){
	                Log.printLine("Warning: Please replace daxPath with the physical path in your working environment!");
	                return;
	            }
	            
	            // TODO partitioning in scheduler
	            Parameters.SchedulingAlgorithm sch_method = Parameters.SchedulingAlgorithm.DYNAMIC_RND; 
	            Parameters.PlanningAlgorithm pln_method = Parameters.PlanningAlgorithm.INVALID; 
	            
	            // TODO LOCAL causes issues but SHARED may be worse for transfer cost computation 
	            ReplicaCatalog.FileSystem file_system = ReplicaCatalog.FileSystem.SHARED; 
	            
	            /**
	             * No overheads //TODO ?
	             */
	            OverheadParameters op = new OverheadParameters(0, null, null, null, null, 0);;
	            
	            /**
	             * No Clustering //TODO ?
	             */
	            ClusteringParameters.ClusteringMethod method = ClusteringParameters.ClusteringMethod.NONE;
	            ClusteringParameters cp = new ClusteringParameters(0, 0, method, null);
	            
	            /**
	             * Initialize static parameters
	             */
	            Parameters.init(NUM_SITES, daxPath, null,
	                    null, op, cp, sch_method, pln_method,
	                    null, 0);
	            ReplicaCatalog.init(file_system);
	          
	            
	            
	            // enable thresholds for scheduling
	            Parameters.enable_thresholds(); 
	            Parameters.setTASK_THRESHOLD(TASK_THRESHOLD);
	            Parameters.setSEC_THRESHOLD(SEC_THRESHOLD);
	            Parameters.setTHRESHOLD_CHECKING_INTERVAL(THRESHOLD_CHECKING_INTREVAL);
	            
	            // before creating any entities.
	            int num_user = 1;   // number of grid users
	            Calendar calendar = Calendar.getInstance(); 
	            boolean trace_flag = false;  // mean trace events 
	            
	            // Initialize the CloudSim library
	            CloudSim.init(num_user, calendar, trace_flag);
	            
	            List<WorkflowDatacenter> workflowDatacenters = new LinkedList<>();
	            // Create sites
	            for ( int i = 0; i< NUM_SITES; i++) {
	            	workflowDatacenters.add(createDatacenter("Datacenter_" + i));
	            }
	            
	            /**
	             * Create a WorkflowPlanner with one schedulers. 
	             */
	            WorkflowPlanner wfPlanner = new WorkflowPlanner("planner_0", 1);
	            /**
	             * Create a WorkflowEngine.
	             */
	            WorkflowEngine wfEngine = wfPlanner.getWorkflowEngine();
	            
	            /**
	             * Create a list of VMs.The userId of a vm is basically the id of the scheduler
	             * that controls this vm.
	             * Note: vmlist[i] = vm at workflowDatacenters[i] 
	             */
	            List<CondorVM> vmList = createVM(wfEngine.getSchedulerId(0), NUM_SITES);
	            
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
	            
	            
	            // TODO add Network Properties (change bandwith, latency)
	            // NetworkTopology.addLink(datacenter0.getId(), datacenter1.getId(), 10.0, 10);
	            

	            CloudSim.startSimulation();


	            List<Job> outputList0 = wfEngine.getJobsReceivedList();

	            CloudSim.stopSimulation();
	            
	            printJobList(outputList0); // TODO maybe change that for better logging/measurements
	            
	            
		} catch (Exception e) {
            Log.printLine("The simulation has been terminated due to an unexpected error");
        }
	}
	
	protected static WorkflowDatacenter createDatacenter(String name) {
		 // We need to create a list to store one or more Machines
        List<Host> hostList = new ArrayList<Host>();
        
        // create host = physical machine hosting VMs
        // we need one host per datacenter only
        // Pe = processing unit
        
        List<Pe> peList = new ArrayList<Pe>();
        for (int i=0;i<PES;i++) {
        	peList.add(new Pe(i, new PeProvisionerSimple(MIPS))); // TODO should i really use overall MIPS here?
        }
        
        int hostId  = 0;
        int ram     = RAM;
        int storage = STORAGE;
        int bw      = 10000; // TODO
        
        hostList.add(
        		new Host(
        				hostId, 
        				new RamProvisionerSimple(ram), 
        				new BwProvisionerSimple(bw), // TODO
        				storage, 
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
        
        
        // TODO cost model?
        double cost = 3.0;              // the cost of using processing in this resource
        double costPerMem = 0.05;		// the cost of using memory in this resource
        double costPerStorage = 0.1;	// the cost of using storage in this resource
        double costPerBw = 0.1;			// the cost of using bw in this resource
        
        
        WorkflowDatacenter datacenter = null;
        DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
                arch, os, vmm, hostList, time_zone, cost, costPerMem, costPerStorage, costPerBw);
        
        

        LinkedList<Storage> storageList = new LinkedList<Storage>();	//we are not adding SAN devices by now
        // create a cluster storage object.
        /**
         * The bandwidth between data centers.
         */
        // TODO 
        double interBandwidth = 1.5e7;// the number comes from the futuregrid site, you can specify your bw
        //double interBandwidth = 1.5e1; (very bad bandwitdh)
        
        /**
         * The bandwidth within a data center.
         */
        double intraBandwidth = interBandwidth * 2;
        try {
        	
        	// IMPORTANT keep name as it is here so that storages have the same name as their datacenters
        	ClusterStorage s1 = new ClusterStorage(name, 1e12); // TODO: the capacity has to be specified for each cluster!

			// TODO datacenter-wise storage capacity is configurable like this:

			if (name.equals("Datacenter_1")) {
				s1 = new ClusterStorage(name, 1000);
			}

			if (name.equals("Datacenter_0")) {
                /**
                 * The bandwidth from Datacenter_0 to Datacenter_1.
                 */
                s1.setBandwidth("Datacenter_1", interBandwidth);

            } else if (name.equals("Datacenter_1")) {
                /**
                 * The bandwidth from Datacenter_1 to Datacenter_0.
                 */
                s1.setBandwidth("Datacenter_0", interBandwidth);

            }
            
            // The bandwidth within a data center
            s1.setBandwidth("local", intraBandwidth);
            // The bandwidth to the source site 
            s1.setBandwidth("source", interBandwidth); //TODO check how/if this is used -> for initial stagein
            storageList.add(s1);
        	
            datacenter = new WorkflowDatacenter(name, characteristics, new VmAllocationPolicySimple(hostList), storageList, 0);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return datacenter;
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
        Log.printLine("Cloudlet ID" + indent + "STATUS" + indent
                + "Data center ID" + indent + "VM ID" + indent + indent + "Time" + indent + "Start Time" + indent + "Finish Time" + indent + "Depth");

        DecimalFormat dft = new DecimalFormat("###.##");
        for (int i = 0; i < size; i++) {
            job = list.get(i);
            Log.print(indent + job.getCloudletId() + indent + indent);

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
}