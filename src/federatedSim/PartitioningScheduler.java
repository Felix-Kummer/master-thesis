package federatedSim;

import federatedSim.utils.DisjointSetUnion;
import org.workflowsim.scheduling.BaseSchedulingAlgorithm;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

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

	@Override
	public void run() throws Exception {
		// TODO Auto-generated method stub
		// TODO: handle when storage is (unexpectedly exceeded)

	}

}
