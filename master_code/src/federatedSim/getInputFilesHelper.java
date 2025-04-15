package federatedSim;

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

import java.io.File;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.*;

public class getInputFilesHelper {

	static int NUM_SITES = 0; // number of sites

	//static double BW = 1.5e7;
	static double BW = 0;
	
	 // Partitioning Thresholds
    static int TASK_THRESHOLD;
    static int SEC_THRESHOLD;
    static double THRESHOLD_CHECKING_INTERVAL = 1; // in which interval the thresholds are checked



	public static void main(String[] args) {

		Log.disable();

		// parse config File
		if (args.length != 2) {
			throw new RuntimeException("Need a dax file and the path were the output should be written");
		}

		try {
		
		// Load workflow

			String daxPath = args[0];
			if(daxPath == null){
				Log.printLine("[DONE]  Warning: Please replace daxPath with the physical path in your working environment!");
				return;
			}
			File daxFile = new File(daxPath);
			if(!daxFile.exists()){
				Log.printLine("Warning: Please replace daxPath with the physical path in your working environment!");
				return;
			}

			

			Parameters.SchedulingAlgorithm sch_method = Parameters.SchedulingAlgorithm.DYNAMIC_RND;


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


			CloudSim.startSimulation();


			List<Job> jobList = wfEngine.getJobsList();


			HashSet<String> jobOutputFiles = new HashSet<>();
			HashMap<String, Integer> jobInputFiles  = new HashMap<>();

			long totalSize = 0;
			long totalInputSize = 0;

			// iterate all jobs and all files
			for (Job job : jobList) {
				for (Object o : job.getFileList()) {
					org.cloudbus.cloudsim.File file = (org.cloudbus.cloudsim.File) o;
					if (file.getType() == Parameters.FileType.INPUT.value) {
						if (jobInputFiles.containsKey(file.getName())) {
							continue;
						}
						totalSize+=file.getSize();
						totalInputSize+=file.getSize();
						jobInputFiles.put(file.getName(), file.getSize() );
					}
					else if (file.getType() == Parameters.FileType.OUTPUT.value) {
						if (jobOutputFiles.contains(file.getName())) {
							continue;
						}
						totalSize+=file.getSize();
						jobOutputFiles.add(file.getName());
					}
				}
			}

			try (PrintWriter writer = new PrintWriter(args[1])) {
				writer.println("TOTALSIZE,"+totalSize);
				writer.println("INPUTSIZE,"+totalInputSize);
				// iterate input files and write to csv
				for (String inputName : jobInputFiles.keySet()) {
					if (jobOutputFiles.contains(inputName)) {
						continue;
					} else {
						writer.println(inputName + "," + jobInputFiles.get(inputName));
					}
				}
			}


			CloudSim.stopSimulation();

		} catch (Exception e) {
			e.printStackTrace();
            Log.printLine("The simulation has been terminated due to an unexpected error");
        }
	}

	

}