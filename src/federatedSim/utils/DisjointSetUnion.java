package federatedSim.utils;

import java.util.HashMap;
import java.util.Map;

public class DisjointSetUnion {
    // parent (graph structure), has-parent relation (parent.put(x,y) = x has parent y)
    private final Map<Integer, Integer> parent = new HashMap<>();

    // used to merge sets (determine merge order)
    private final Map<Integer, Integer> rank   = new HashMap<>();

    // return whether the integer is contained in any set
    public boolean contains (int x) {
        return parent.containsKey(x);
    }

    // add new object in its own group
    public void add(int x) {
        if (!contains(x)){
            parent.put(x,x); // this is how we determine representatives of a set (if find(x) = x)
            rank.put(x,0);
        }
    }


    // find the representative of group containig x
    public int find(int x) {
        if (!contains(x)){
            throw new IllegalArgumentException("Element not found:" + x);
        }
        // x is not the representative -> find it
        if (parent.get(x) != x) {
            // make x a direct child of its representative to trim the future search space (aka. path compression)
            parent.put(x, find(parent.get(x)));
        }

        // return either self (when x is representative) or found representative (which is now x's parent)
        return parent.get(x);
    }

    public void union(int x, int y){

        // find representatives/roots of x and y
        int representativeX = find(x);
        int representativeY = find(y);

        // if they don't already share the same representative, we need to merge them
        if (representativeX != representativeY) {

            int rankX = rank.get(representativeX);
            int rankY = rank.get(representativeY);

            // Union by rank, this is done to ensure that smaller trees are always merged into the larger trees
            if (rankX > rankY) {
                parent.put(representativeY, representativeX);
            } else if (rankX < rankY) {
                parent.put(representativeX, representativeY);
            } else {
                // same rank -> make x-tree parent of y-tree and increments x-tree's rank accordingly
                parent.put(representativeY, representativeX);
                rank.put(representativeX,  rankX + 1);
            }
        }
    }

    // check if two elements share the same representative
    public boolean inSameGroup(int x, int y) {
        if (!contains(x) || ! contains(y)) {
            return false;
        }
        return find(x) == find(y);
    }

    // tests
    public static void main(String[] args) {
        DisjointSetUnion dsu = new DisjointSetUnion();
        dsu.add(1);
        dsu.add(2);
        dsu.add(3);
        dsu.union(1, 2);
        System.out.println(dsu.inSameGroup(1, 2)); // true
        System.out.println(dsu.inSameGroup(1, 3)); // false
        dsu.union(2, 3);
        System.out.println(dsu.inSameGroup(1, 3)); // true
    }
}
