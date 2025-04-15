package federatedSim;

import java.util.*;
import java.util.Arrays;

import org.cloudbus.cloudsim.File;
import org.workflowsim.Job;
import org.workflowsim.Task;

import org.workflowsim.utils.Parameters;

public class DynamicAbstractDag {
	
	private static HashMap<String, DynamicAbstractNode> name2node;

	public static void init(List<Task> tasks) {
		name2node = new HashMap<>();
		
		
		for (Task task : tasks) {
			
			// we don't know the task yet, add it and all its dependencies
			if (!name2node.containsKey(task.getType())){
				DynamicAbstractNode dynamicAbstractNode = new DynamicAbstractNode(task);
				name2node.put(task.getType(), dynamicAbstractNode);
			}
		}
	}
	
	private static DynamicAbstractNode addOrReturnNode(Task task)  {
		String name = task.getType();
		if (name2node.containsKey(name)) {
			return name2node.get(name);
		} else {
			DynamicAbstractNode newNode = new DynamicAbstractNode(task);
			name2node.put(name, newNode);
			return newNode;
		}
	}


	// list of start notes, these are required to launch propagation after dag updates
	private static List<DynamicAbstractNode> startNodes = new ArrayList<>();


	public static void processInitialPartitioning(List<Job> jobs) {

		// maps job names to task-per-site
		Map<String, int[]> name2tps = new HashMap<>();

		for (Job job : jobs) {
			if (job.getParentList().size() > 0) { // ensure that these tasks are actually start tasks
				throw new RuntimeException("Tried to update the dynamic Dag's start tasks with task that have parents ");
			}

			// remember job counts

			Task actualTask = job.getTaskList().get(0);

			int vmId = job.getVmId(); // outer job holds VM ID

			String taskName = actualTask.getType(); // inner task hold name

			// update mapping
			int[] taskPerSite = name2tps.computeIfAbsent(taskName, name -> new int[Parameters.getVmNum()]);
			taskPerSite[vmId] += 1;

			// also update information about input files
			updateInputSizes(job, name2node.get(taskName));
		}

		// init nodes with initial partitioning as 'predictions', also capture start notes for future propagation
		for (Map.Entry<String, int[]> entry : name2tps.entrySet()){

			// init initial jobs
			DynamicAbstractNode jobNode = name2node.get(entry.getKey());
			jobNode.setTaskEstimates(entry.getValue());

			// propagate changes through dynamic DAG
			jobNode.propagate(new int[0], new double[0]); // we don't need to pass values here because these are start nodes and we don't have anything to propagate upwards atm.

			// add start note for later updates
			startNodes.add(jobNode);
		}
	}

	/**
	 * Update dynamic 1. output sizes, 2. dependency structures, 3. predicted task counts
	 */
	public static void updateDynamicInformation(List<Job> receivedJobList, List<Job> newJobs) {
		processFinishedJobs(receivedJobList); // updated based on finished/received jobs

		processNewJobs(newJobs); // update input file sizes and dependency structures

		// propagate changes
		for (DynamicAbstractNode startNode : startNodes) {
			startNode.propagate(new int[0], new double[0]); // empty array because these are start nodes and there is no basis to propaget from
		}
	}

	// store the length of the previous received list, using this, we can determine task that finished after the last
	// update of dynamic information
	private static int numReceivedTasks = 0;

	// update dynamic output size of finished tasks
	// also updated the dependency structure from finished jobs to their children
	// we can derive the children here, because these jobs have been processed by the WfEngine and we therefore now know which children it has
	private static void processFinishedJobs(List<Job> receivedJobList) {

		int newNumReceivedTasks = receivedJobList.size();

		// no new jobs, were done
		if (numReceivedTasks > newNumReceivedTasks){
			return;
		}


		for (int i = numReceivedTasks; i < newNumReceivedTasks; i++ ) {

			// get the actual task which is nested at this stage
			Task task = receivedJobList.get(i).getTaskList().get(0);

			// match to node
			DynamicAbstractNode node = name2node.get(task.getType());

			// get outputSize
			int outputSize = 0;
			for (File file : (List<File>) task.getFileList()){
				if (file.getType() == Parameters.FileType.OUTPUT.value) {
					outputSize += file.getSize();
				}
			}

			// update average output size
			node.updateAverageOutputSize(outputSize);

			// remember number child jobs generated from this jobs by abstract type
			HashMap<String, Integer> childCount = new HashMap<>();

			// iterate child nodes
			for (Task c : receivedJobList.get(i).getChildList()) {
				// get child job and name
				Job childJob = (Job) c;
				String childName = childJob.getTaskList().get(0).getType();

				// increase count
				childCount.put(childName, childCount.getOrDefault(childName, 0) + 1);
			}

			// iterate childCounts
			for (String childName : childCount.keySet()) {

				// get edge from finished job to children
				DynamicAbstractEdge childEdge = node.getChildrenEdges().get(childName);

				// update dependency structure
				childEdge.updateChildren2ParentsDependencyType(childCount.get(childName));
			}

		}

		// update received job count
		numReceivedTasks = newNumReceivedTasks;
	}



	private static void processNewJobs(List<Job> newJobs) {


		for (Job job: newJobs) {
			// get the node
			DynamicAbstractNode jobNode = name2node.get(job.getTaskList().get(0).getType());

			// update input file sizes (and get input files)
			Map<String, Integer> inputFiles = updateInputSizes(job, jobNode);

			// update dependency structure, includes transfer sizes
			updateDependencyStructures(job, jobNode, inputFiles);
		}
	}


	private static void updateDependencyStructures(Job newJob, DynamicAbstractNode childJobNode, Map<String, Integer> inputFiles) {
		// get parents
		List<Job> parentJobs = newJob.getParentList();

		// extract dependency structure and transfer sizes
		HashMap<DynamicAbstractNode, Integer> deps = new HashMap<>();
		HashMap<DynamicAbstractNode, Double>  transferSizes = new HashMap<>();
		for (Job parent : parentJobs) {
			DynamicAbstractNode parentNode = name2node.get(parent.getTaskList().get(0).getType());
			deps.put(parentNode, deps.getOrDefault(parentNode, 0) + 1 );

			// find which input file this parent provides and update transfer costs
			for (Object o : parent.getFileList()) {
				File parentOutputFile = (File) o;
				String parentOutputFileName = parentOutputFile.getName();
				if (parentOutputFile.getType() == Parameters.FileType.OUTPUT.value && inputFiles.containsKey(parentOutputFileName)) { // is this file an output of parent that is an input of the current job
					transferSizes.put(parentNode, transferSizes.getOrDefault(parentNode, 0.0 ) + inputFiles.get(parentOutputFileName));
				}
			}
		}

		// update dependency structure
		for (DynamicAbstractNode parentNode : deps.keySet()){
			DynamicAbstractEdge edge = parentNode.getChildrenEdges().get(childJobNode.getName());
			edge.updateParents2ChildrenDependencyType(deps.get(parentNode));
			edge.updateTransferSize(transferSizes.getOrDefault(parentNode, 0.0), deps.get(parentNode));
		}
	}

	private static Map<String, Integer> updateInputSizes(Job newJob, DynamicAbstractNode jobNode) {
		// bookkeeping of input files for other updates
		HashMap<String, Integer> inputFileNames  = new HashMap();


		int inputSize = 0;
		for (File file : (List<File>) newJob.getFileList()) {
			if (file.getType() == Parameters.FileType.INPUT.value) {
				inputSize += file.getSize();
				inputFileNames.put(file.getName(), file.getSize());
			}
		}

		jobNode.updateAverageInputSize(inputSize);

		return inputFileNames;
	}

	// returns the total estimated size (input + output), per site, combining each estimated job for a given job's type
	public static double[] getEstimatedSize4JobType(Job job) {
		// get job name
		String jobName = job.getTaskList().get(0).getType();

		return name2node.get(jobName).getSizeEstimates();
	}

	// returns the estimated size (input + output) for a single job of the give job's type
	public static double getEstimatedSize4Job(Job job) {
		// get job name
		String jobName = job.getTaskList().get(0).getType();

		DynamicAbstractNode node = name2node.get(jobName);

		return node.getAssumedOrPredictedInputSize() + node.getAssumedOrPredictedOutputSize();
	}

	// returns the estimated total size of each site mapping, including all jobs
	public static double[] getTotalSizeEstimates() {

		int numSites = Parameters.getVmNum();

		double[] aggregates = new double[numSites];

		// iterate all know job types (=names)
		for (DynamicAbstractNode node : name2node.values()) {
			// get site-wise estimates for all estimated jobs
			double[] estimates = node.getSizeEstimates();

			// aggregate
			for (int i = 0; i < numSites; i++){
				aggregates[i] += estimates[i];
			}
		}

		return aggregates;
	}

	// returns the estimated size of a single Job of job's type and its entire subtree (children, children of children ,...)
	public static double getEstimatedSize4SubDAG(Job job) {
		return name2node.get(job.getTaskList().get(0).getType()).getSubtreeSize(new HashSet<>());
	}

	// returns the estimated output sizes for a job's Job type
	public static double getEstimatedOutputSize(Job job) {
		return name2node.get(job.getTaskList().get(0).getType()).getAssumedOrPredictedOutputSize();
	}

	// returns the estimated input sizes for a job's Job type
	public static double getEstimatedInputSize(Job job) {
		return name2node.get(job.getTaskList().get(0).getType()).getAssumedOrPredictedInputSize();
	}

	// update shift array for node of job's type where increaseId site will get +1 and decreaseId will get -1
	// this is used in the partitioner to signal job assignments that a contrary to the best node assignment
	public static void updateShiftArray(Job job, int increaseId, int decreaseId) {
		DynamicAbstractNode jobNode = name2node.get(job.getTaskList().get(0).getType());

		jobNode.updateShiftArray(increaseId, decreaseId);


	}


	private static class DynamicAbstractNode {
		
		private HashMap<String, DynamicAbstractEdge> parentsEdges  = new HashMap<>();
		private HashMap<String, DynamicAbstractEdge> childrenEdges = new HashMap<>();

		private String name;

		public HashMap<String, DynamicAbstractEdge> getParentsEdges() {
			return parentsEdges;
		}

		public HashMap<String, DynamicAbstractEdge> getChildrenEdges() {
			return childrenEdges;
		}

		protected DynamicAbstractNode(Task task) {
			name = task.getType();

			int numSites = Parameters.getVmNum();

			
			List<Task> children = task.getChildList();
			resolveTasks(children, true);

			//List<Task> parents  = task.getParentList();
			// resolveTasks(parents, false);
			
		}

		/**
		 * This function gets a list of Task objects that are either children or parents of the current node.
		 * If the nodes do not already exist in the DAG name2node, we add the nodes.
		 * It then creates a link for each task on the list with this node. 
		 * 
		 * @param tasks List of tasks to create links with this node.
		 * @param children whether the list are children, if false -> parent
		 */
		private void resolveTasks(List<Task> tasks, boolean children) {
			for (Task task : tasks) {
				DynamicAbstractNode node = addOrReturnNode(task);
				maybeAddEdge(children, node);
			}
			
		}
		
		public String getName() { return this.name; }
		
		public void addChildrenEdge(DynamicAbstractEdge edge) throws Exception {
			String childName  = edge.getNode2().getName();
			String parentName = edge.getNode1().getName();
			
			if (!parentName.equals(this.name)) {
				throw new Exception("Wrong node.");
			} 
			
			if (childrenEdges.containsKey(childName)) {
				throw new Exception("Tried to insert known edge");
			}
			
			childrenEdges.put(childName, edge);
		}
		
		public void addParentEdge(DynamicAbstractEdge edge) throws Exception {
			String childName  = edge.getNode2().getName();
			String parentName = edge.getNode1().getName();
			
			if (!childName.equals(this.name)) {
				throw new Exception("Wrong node.");
			} 
			
			if (parentsEdges.containsKey(parentName)) {
				throw new Exception("Tried to insert known edge");
			}
			
			parentsEdges.put(parentName, edge);
		}
		
		private void maybeAddEdge(boolean children, DynamicAbstractNode node) {
			
			// get correct map
			HashMap<String, DynamicAbstractEdge> map;
			if (children) {
				map = childrenEdges;
			} else {
				map = parentsEdges;
			}
			
			String nodeName = node.getName();
			
			// if we know the dependency -> nothing to do
			if (map.containsKey(nodeName)) {
				return;
				
			// we got a new dependency -> create edge and add that to both nodes
			} else {
				DynamicAbstractEdge edge;
				if (children) {
					edge = new DynamicAbstractEdge(this, node);
					try {
						this.addChildrenEdge(edge);
						node.addParentEdge(edge);
					} catch (Exception e) {
						e.printStackTrace();
						System.exit(1);
					}
				} else {
					edge = new DynamicAbstractEdge(node, this);
					try {
						this.addParentEdge(edge);
						node.addChildrenEdge(edge);
					} catch (Exception e) {
						e.printStackTrace();
						System.exit(1);
					}
				}
			}
			
		}

		// count physically finished instances
		private int receivedCount = 0;

		private double averageOutputSize = 0;

		private void updateAverageOutputSize(int size) {
			receivedCount ++;
			averageOutputSize = (averageOutputSize*(receivedCount-1) + size)/receivedCount;
		}

		// physically submitted instances
		private int submittedCount = 0;

		private double averageInputSize = 0;

		private void updateAverageInputSize(int size) {
			submittedCount ++;
			averageInputSize = (averageInputSize*(submittedCount-1) + size)/submittedCount;
		}


		// stores the amount of physical tasks predicted for this task type
		private int[] taskEstimates = new int[Parameters.getVmNum()];

		public int[] getTaskEstimates() {
			return taskEstimates;
		}

		public void setTaskEstimates(int[] taskEstimates) {
			this.taskEstimates = taskEstimates;
		}


		// stores the amount of tasks that deviate form a site-continued-partitioning
		// e.g. mayority of parents where on site A, but was assigned to site B
		// In that case shiftArray[siteA]-= 1 and shiftArray[siteB] += 1
		private int[] shiftArray = new int[Parameters.getVmNum()];

		// count parent edges that have propagated their estimates
		private int propagationCount = 0;

		// bookkeeping for highest transfer size from children
		// we use this to keep taskEstimates for the predicted best continuation
		private double[] highestTransferSizes = new double[Parameters.getVmNum()];

		// propagate task counts estimates downstream (from parents to children)
		protected void propagate(int[] newTaskNumberEstimates, double[] transferSizes){

			// increase propagation counter
			propagationCount++;

			int parentSize = getParentsEdges().size();

			// only compute new estimates if this is not a start node
			if (parentSize != 0) {
				// last parent propagated  -> this can propagate
				if (propagationCount == parentSize) {
					// compare known with new estimates, keep higher number
					compareTaskEstimates(newTaskNumberEstimates, transferSizes);

					// reset propagation counter and transfersizes
					propagationCount = 0;
					highestTransferSizes = new double[Parameters.getVmNum()];

					// add shift offset from actual partitioning
					for (int i = 0; i < Parameters.getVmNum(); i++) {
						taskEstimates[i] += shiftArray[i];
					}
					// propagate through child edges
					propagateToChildren(taskEstimates);

				// first parent propagated
				} else if (propagationCount == 1) {
					taskEstimates = newTaskNumberEstimates;

				} else if ( propagationCount < parentSize) {
					// compare known with new estimates, keep higher number
					compareTaskEstimates(newTaskNumberEstimates, transferSizes);
				} else {
					throw new RuntimeException("Error in propagating task estimates");
				}

			// special case: starter nodes
			} else {

				// we can't modify task arrays directly here, as this would change it every iteration
				// we should only modify the shiftArray for starter jobs
				int [] estimates = Arrays.copyOf(taskEstimates, taskEstimates.length);

				// add shift offset, this being != 0 is possible if a start job was retried
				for (int i = 0; i < Parameters.getVmNum(); i++) {
					estimates[i] += shiftArray[i];
				}
				propagateToChildren(estimates);
			}

		}

		// helper function that compares and stores the number of tasks per side
		// it favors the newEstimates for a site, if it would transfer more data than previously -> this mean less data is moved across sites
		private void compareTaskEstimates(int[] newEstimates, double[] transferSizes) {
			for (int i = 0; i < Parameters.getVmNum(); i++){
				if (highestTransferSizes[i] < transferSizes[i]) {
					taskEstimates[i]        = newEstimates[i];
					highestTransferSizes[i] = transferSizes[i];
				};
			}
		}

		// update shift count to reflect changes in the real partitioning
		protected void updateShiftArray(int increaseId, int decreaseId) {
			shiftArray[increaseId] += 1;
			shiftArray[decreaseId] -= 1;
		}


		// estimates for the total size of this job on each site
		private double[] sizeEstimates = new double[Parameters.getVmNum()];

		public double[] getSizeEstimates() {
			return sizeEstimates;
		}



		// these fields hold the assumed output and input sizes
		// this is important if no real data is available yet and a children/the partitioner need the size for their own size predictions/partitioning
		private double assumedOutputSize = 0;
		private double assumedInputSize = 0;

		// return the assumed output size (if no output estimate is available) or the predicted size
		public double getAssumedOrPredictedOutputSize() {
			if (receivedCount == 0) {
				return assumedOutputSize;
			} else {
				return averageOutputSize;
			}

		}

		public double getAssumedOrPredictedInputSize() {
			if (submittedCount == 0){
				return assumedInputSize;
			} else {
				return averageInputSize;
			}
		}

		// estimates the input size of this node from parents (possibly assumed) output size
		// this is required when no input sizes are available yet
		private double assumeInputSize() {

			double inputSize = 0.0;

			// iterate parents
			for (DynamicAbstractEdge parentEdge : parentsEdges.values()) {
				DynamicAbstractNode parentNode = parentEdge.getNode1();

				// get parents output size
				double parentOutput = parentNode.getAssumedOrPredictedOutputSize();

				// get number of children of this parent
				int parentsChildren = parentNode.getChildrenEdges().size();

				// assume output = parentOutput/parentsChildren
				inputSize += parentOutput/parentsChildren;

			}
			return inputSize;
		}

		// helper function to propagate values to children
		private void propagateToChildren(int[] estimates) {
			// update size estimates along the way (required because the Partitioner may query size estimates after every full DAG propagation)
			 double inputSize  = averageInputSize;
			 double outputSize = averageOutputSize;

			// special case: we don't have input estimates yet
			if (submittedCount == 0) {
				// we need to assume inputs from parents outputs
				inputSize = assumeInputSize();

				// if no job was submitted, non can be received -> we need to assume the output size too (we assume output = input)
				outputSize = inputSize;

				// store assumed sizes
				assumedInputSize = inputSize;
				assumedOutputSize = outputSize;

			// second special case: we don't have output, but input estimates
			} else if (receivedCount == 0) {
				// in this case, we just assume a 1-to-1 mapping from inputs to outputs
				outputSize = inputSize;

				// store assumed sizes
				assumedInputSize = inputSize;
				assumedOutputSize = outputSize;
			}

			for (int i = 0; i < Parameters.getVmNum(); i++ ){
				sizeEstimates[i] = (double) taskEstimates[i] * (inputSize + outputSize);
			}

			// propagate over every edge to children
			for (DynamicAbstractEdge childEdge : childrenEdges.values()){
				childEdge.propagate(estimates);
			}
		}

		// get size of subtree starting at single instance of this job
		protected double getSubtreeSize(Set<String> knownNames) {
			// check if this node was already considered
			if (knownNames.contains(name)) {
				return 0.0;
			}

			// add own name to jobs that have already contributed
			knownNames.add(name);

			// add own size
			double size = getAssumedOrPredictedInputSize() + getAssumedOrPredictedOutputSize();

			// add children's subtree sizes
			for (DynamicAbstractEdge childrenEdge : childrenEdges.values()){
				size += childrenEdge.getNode2().getSubtreeSize(knownNames) * childrenEdge.getChildrenToParentsDependencyType() / childrenEdge.getParentsToChildrenDependencyType() ;
			}
			return size;
		}

	}

	private static class DynamicAbstractEdge {
		
		// node1 -> node 2
		private DynamicAbstractNode node1;
		private DynamicAbstractNode node2;
		
		protected DynamicAbstractEdge(DynamicAbstractNode n1, DynamicAbstractNode n2) {
			this.node1 = n1; // parent
			this.node2 = n2; // children
		}
		
		
		@Override 
		public boolean equals(Object object) {
			if (this == object) return true;
			if (object == null || getClass() != object.getClass()) return false;
			DynamicAbstractEdge edge = (DynamicAbstractEdge) object;
			return (this.node1.getName().equals(edge.node1.getName()) && this.node2.getName().equals(edge.node2.getName()));
		}
		
		@Override
		public int hashCode() {
			return Objects.hash(node1.getName(), node2.getName());
		}
		
		public DynamicAbstractNode getNode1() { return node1; }
		public DynamicAbstractNode getNode2() { return node2; }

		// type of dependency,
		// index 0: average parents per one children
		// index 1: average children per one parent
		private double[] dependencyTypes = {1.0, 1.0};

		// counts for running averages, indexes relate to the same relationship as in dependencyTypes
		private int[] dependencyCounts = {0 ,0};

		public void updateParents2ChildrenDependencyType(int count) {
			dependencyCounts[0] +=1;
			dependencyTypes[0] = ((dependencyTypes[0] * (dependencyCounts[0]-1)) + count)/dependencyCounts[0];
		}

		public void updateChildren2ParentsDependencyType(int count) {
			dependencyCounts[1] +=1;
			dependencyTypes[1] = ((dependencyTypes[1] * (dependencyCounts[1]-1)) + count)/dependencyCounts[1];
		}

		public double getParentsToChildrenDependencyType() {
			return dependencyTypes[0];
		}

		public double getChildrenToParentsDependencyType() {
			return dependencyTypes[1];
		}

		private double transferSize = 0.0;

		private int transferSizeUpdateCount = 0;
		public void updateTransferSize(double transferSizeUpdate, int count) {
			transferSizeUpdateCount += count;
			transferSize = ((transferSize * (transferSizeUpdateCount-count)) + transferSizeUpdate)/transferSizeUpdateCount;

		}

		// propagate task estimates and transfer size upward (from paren to children) by multiplying them with this.dependencyType
		protected void propagate (int[] estimates) {
			int[] correctedEstimates = new int[estimates.length];
			double[] transferSizes = new double[estimates.length];
			for (int i = 0; i < estimates.length; i++ ) {
				correctedEstimates[i] = (int) Math.round(estimates[i] / dependencyTypes[0] * dependencyTypes[1]);
				transferSizes[i] = transferSize * dependencyTypes[1];
			}


			node2.propagate(correctedEstimates, transferSizes);
		}


		
	}
	
}

