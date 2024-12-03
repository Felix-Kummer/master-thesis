package federatedSim;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.jar.Attributes.Name;

import javax.naming.spi.Resolver;

import org.workflowsim.Task;

import javafx.scene.Node;

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
	
	
	
	private static class DynamicAbstractNode {
		
		private HashMap<String, DynamicAbstractEdge> parentsEdges  = new HashMap<>();
		private HashMap<String, DynamicAbstractEdge> childrenEdges = new HashMap<>();
		
		// count task instances
		private int count = 0;
		private String name;
		
		protected DynamicAbstractNode(Task task) {
			name = task.getType();
			
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
		
	}
	
}

