package federatedSim;

import java.util.*;

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

		// maps job names to task-per-site lost
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
			jobNode.setTaskMappingCount(entry.getValue());

			// propagate changes through dynamic DAG
			jobNode.propagate(new int[0]); // we don't need to pass values here because these are start nodes and we don't have anything to propagate upwards atm.

			// add start note
			startNodes.add(jobNode);

		}
	}

	/**
	 * Update dynamic 1. output sizes, 2. dependency structures, 3. predicted task counts
	 */
	public static void updateDynamicInformation(List<Job> receivedJobList, List<Job> newJobs) {
		updateOutputSizes(receivedJobList); // updated based on finished/received jobs

		processNewJobs(newJobs); // update input file sizes and dependency structures

		// propagate changes
		for (DynamicAbstractNode startNode : startNodes) {
			startNode.propagate(new int[0]); // empty array because these are start nodes and there is no basis to propaget from
		}



	}

	// store the length of the previous received list, using this, we can determine task that finished after the last
	// update of dynamic information
	private static int numReceivedTasks = 0;

	// update dynamic output size of tasks
	private static void updateOutputSizes(List<Job> receivedJobList) {

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
		}

		// update received job count
		numReceivedTasks = newNumReceivedTasks;
	}



	private static void processNewJobs(List<Job> newJobs) {


		for (Job job: newJobs) {
			// get the node
			DynamicAbstractNode jobNode = name2node.get(job.getTaskList().get(0).getType());

			// update dependency structure
			updateDependencyStructures(job, jobNode);

			// update input file sizes
			updateInputSizes(job, jobNode);


		}
	}


	private static void updateDependencyStructures(Job newJob, DynamicAbstractNode childJobNode) {
		// get parents
		List<Job> parentJobs = newJob.getParentList();

		// extract dependency structure
		HashMap<DynamicAbstractNode, Integer> deps = new HashMap<>();
		for (Job parent : parentJobs) {
			DynamicAbstractNode parentNode = name2node.get(parent.getTaskList().get(0).getType());
			deps.put(parentNode, deps.getOrDefault(parentNode, 0) + 1 );
		}

		// update dependency structure
		for (DynamicAbstractNode parentNode : deps.keySet()){
			DynamicAbstractEdge edge = parentNode.getChildrenEdges().get(childJobNode.getName());
			edge.updateDependencyType(deps.get(parentNode));
		}
	}

	private static void updateInputSizes(Job newJob, DynamicAbstractNode jobNode) {
		int inputSize = 0;
		for (File file : (List<File>) newJob.getFileList()) {
			if (file.getType() == Parameters.FileType.INPUT.value) {
				inputSize += file.getSize();
			}
		}

		jobNode.updateAverageInputSize(inputSize);
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

		private int averageOutputSize = 0;

		private void updateAverageOutputSize(int size) {
			receivedCount ++;
			averageOutputSize = (averageOutputSize*(receivedCount-1) + size)/receivedCount;
		}

		// physically submitted instances
		private int submittedCount = 0;

		private int averageInputSize = 0;

		private void updateAverageInputSize(int size) {
			submittedCount ++;
			averageInputSize = (averageInputSize*(submittedCount-1) + size)/submittedCount;
		}


		// stores the amount of physical tasks predicted for this task type
		private int[] taskMappingCount = new int[Parameters.getVmNum()];

		public int[] getTaskMappingCount() {
			return taskMappingCount;
		}

		public void setTaskMappingCount(int[] taskMappingCount) {
			this.taskMappingCount = taskMappingCount;
		}


		// stores the amount of tasks that deviate form a site-continued-partitioning
		// e.g. mayority of parents where on site A, but was assigned to site B
		// In that case shiftArray[siteA]-= 1 and shiftArray[siteB] += 1
		private int[] shiftArray = new int[Parameters.getVmNum()];

		// count parent edges that have propagated their estimates
		private int propagationCount = 0;

		// propagate task counts estimates downstream (from parents to children)
		protected void propagate(int[] newEstimates){

			// increase propagation counter
			propagationCount++;

			int parentSize = getParentsEdges().size();

			// only compute new estimates if this is not a start node
			if (parentSize != 0) {

				// first parent propagated
				if (propagationCount == 1) {
					taskMappingCount = newEstimates;

				} else if ( propagationCount < parentSize) {
					// compare known with new estimates, keep higher number
					compareTaskEstimates(newEstimates);

				// last parent propagated  -> this can propagate
				} else if (propagationCount == parentSize) {
					// compare known with new estimates, keep higher number
					compareTaskEstimates(newEstimates);

					// reset propagation counter
					propagationCount = 0;

					// add shift offset form actual partitioning
					for (int i = 0; i < Parameters.getVmNum(); i++){
						taskMappingCount[i] += shiftArray[i];
					}

					// propagate through child edges
					propagateToChildren(taskMappingCount);

				} else {
					throw new RuntimeException("Error in propagating task estimates");
				}

			// special case: starter nodes
			} else {
				propagateToChildren(taskMappingCount); // this is set once for initial nodes and can't change
			}

		}

		// helper function that compares and stores the max number of tasks pre side predicted by parent node dependencies nodes
		private void compareTaskEstimates(int[] newEstimates) {
			for (int i = 0; i < Parameters.getVmNum(); i++){
				if (taskMappingCount[i] < newEstimates[i]) {
					taskMappingCount[i] = newEstimates[i];
				};
			}
		}

		// helper function to propagate values to children
		private void propagateToChildren(int[] estimates) {
			for (DynamicAbstractEdge childEdge : childrenEdges.values()){
				childEdge.propagate(estimates);
			}

		}
	}

	private static class DynamicAbstractEdge {
		
		// node1 -> node 2
		private DynamicAbstractNode node1;
		private DynamicAbstractNode node2;
		
		protected DynamicAbstractEdge(DynamicAbstractNode n1, DynamicAbstractNode n2) {
			this.node1 = n1;
			this.node2 = n2;
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
		// value 1.0 == one child has one parent
		// value 2.0 == one child has two parents
		private double dependencyType = 1.0;

		private int dependencyCount = 0;

		public void updateDependencyType(int count) {
			dependencyCount +=count;
			dependencyType = ((dependencyType * (dependencyCount-count)) + count)/dependencyCount;
		}

		// propagate task estimates upward (from paren to children) by multiplying them with this.dependencyType
		protected void propagate (int[] estimates) {
			int[] correctedEstimates = new int[estimates.length];
			for (int i = 0; i < estimates.length; i++ ) {
				correctedEstimates[i] = (int) Math.round(estimates[i] * dependencyType);
			}
			node2.propagate(correctedEstimates);
		}
		
	}
	
}

