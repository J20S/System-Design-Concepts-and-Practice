import java.util.*;

/**
 * When the amount of data exceeds the storage of a single machine, we need to scale it up.
 * We need multiple machines to store our data, also to prevent single point failure.
 * The technique of splitting data into different machines is called sharding.
 * There are two types of sharding techniques, vertical and horizontal sharding.
 * Vertical sharding is to split data based on tables, whereas horizontal sharding is to split data based on
 * their keys.
 *
 * Vertical sharding has some limitations. For instance, some tables may get really big, some tables may have
 * significantly more reads and writes than others. This will cause imbalanced use of different databases.
 * We tend to use horizontal sharding more often.
 *
 * The naive solution to horizontal sharding is to evenly distribute the data to the number of machines we have.
 * e.g. If we have 10 machines, we find the machine (id % 10) for a given data. The problem of this approach is that
 * when we add a new machine in the future, there is a lot of data we need to migrate, which will take a lot of time,
 * may cause potential data inconsistency and add a lot of pressure to our servers.
 *
 * So how do we do it? Consistent hasing.
 *
 * Imagine we have a data circle where all of our data can be hashed onto a particular point on the circle.
 * We divide this circle into n intervals (points), where n can be a really large number, usually 2^64.
 * For each machine, we assign k random and unique micro shards (treat micro shard as a virtual point on the circle)
 * to it. For each data, we first calculate its hash value (between 0 to n-1 so that it can be mapped to a virtual
 * point on the circle), then this data will be assigned to the machine which owns the next micro shard clockwise of
 * the hash value.
 *
 * With this approach, whenever there is a need to add a new machine, you can expect that on average, only a small
 * portion of data from each existing machine will be migrated to the new machine. This solves the all the problems 
 * I mentioned for the naive approach.
 *
 * Created by Haitao (James) Li on 11/09/2016
 */

public class ConsistentHashing {
    private static ConsistentHashing instance;
    private int intervals;
    private int microShardsPerMachine;
    private Map<Integer, List<Integer>> machine_shards;
    private Set<Integer> microShardsSet;

    private ConsistentHashing(int intervals, int microShardsPerMachine) {
        this.intervals = intervals;
        this.microShardsPerMachine = microShardsPerMachine;
        machine_shards = new HashMap<>();
        microShardsSet = new HashSet<>();
    }
    /**
     * Singleton pattern
     * @param intervals number of intervals of the entire data circle
     * @param microShardsPerMachine number of micro shards for each added machine
     * @return the only instance of ConsistentHashing
     */
    public static ConsistentHashing create(int intervals, int microShardsPerMachine) {
        if (instance == null) {
            synchronized (ConsistentHashing.class) {
                if (instance == null) {
                    instance = new ConsistentHashing(intervals, microShardsPerMachine);
                }
            }
        }
        return instance;
    }

    /**
     * Add a machine, generate microShardsPerMachine number of random micro shards for it, and save them in hashmap.
     * @param machineId machine ID
     */
    public void addMachine(int machineId) {
        List<Integer> shards = new ArrayList<>();
        if (machine_shards.containsKey(machineId)) {
                return;
        }
        machine_shards.put(machineId, shards);
        Random ran = new Random();
        for (int i = 0; i < microShardsPerMachine; i++) {
            int randomShard = ran.nextInt(intervals);
            while (microShardsSet.contains(randomShard)) {
                randomShard = ran.nextInt(intervals);
            }
            shards.add(randomShard);
            microShardsSet.add(randomShard);
        }
        // We sort the micro shards so that we can use binary search to
        // improve the performance of getMachineIdByHashCode
        Collections.sort(shards);
    }

    /**
     * Given a piece of data, the machine it belongs to is the machine
     * that owns the next micro shard for the hashcode of the data.
     * @param hashcode hash value of a piece of data
     * @return machineId
     */
    public int getMachineIdByHashCode(int hashcode) {
        int machineId = -1;
        int minDiff = Integer.MAX_VALUE;
        // we need to find a micro shard with smallest distance clockwise with the hashcode on the circle
        for (int id: machine_shards.keySet()) {
            List<Integer> tempShards = machine_shards.get(id);
            int start = 0, end = tempShards.size() - 1;
            // Since micro shards for each machine have already been sorted, we use binary search
            while (start + 1 < end) {
                int mid = start + (end - start) / 2;
                if (tempShards.get(mid) == hashcode) {
                    return id;
                }
                else if (tempShards.get(mid) < hashcode) {
                    start = mid;
                }
                else {
                    end = mid;
                }
            }
            if (tempShards.get(start) == hashcode || tempShards.get(end) == hashcode) {
                return id;
            }
            // When calculating difference, we also need to consider hashcode is greater than any micro shard
            // In this case, diff = microShard - hashcode + intervals
            int diff = tempShards.get(start) - hashcode;
            diff = diff < 0 ? diff + intervals : diff;
            if (diff < minDiff) {
                minDiff = diff;
                machineId = id;
            }
            diff = tempShards.get(end) - hashcode;
            diff = diff < 0 ? diff + intervals : diff;
            if (diff < minDiff) {
                minDiff = diff;
                machineId = id;
            }
        }
        return machineId;
    }

}
