import java.util.*;

/**
 * Cassandra is a NoSQL database, and it uses two-level keys to find a specific value.
 * In a Cassandra table, each row is determined by a raw_key (hash key / shard key).
 * Each row can have expandable number of columns, columns are sorted by column keys and support
 * range query. Column keys are usually ids or timestamp + id.
 * Created by Haitao (James) Li on 11/09/2016.
 */

public class MiniCassandra<K, S, V> {

    private class Column<K, V> {
        K key;
        V value;

        Column(K key, V value) {
            this.key = key;
            this.value = value;
        }

    }

    // we choose NavigableMap interface because we need to support range query on column keys
    private Map<K, NavigableMap<S, V>> table;

    public MiniCassandra() {
        table = new HashMap<>();
    }

    /**
     * @param raw_key hash key or shard key
     * @param column_key column key
     * @param column_value column value
     */
    public void insert(K raw_key, S column_key, V column_value) {
        if (!table.containsKey(raw_key)) {
            table.put(raw_key, new TreeMap<>());
        }
        table.get(raw_key).put(column_key, column_value);
    }

    /**
     * Range query on column key
     * @param raw_key hash key or shard key
     * @param column_start lower bound of column key
     * @param column_end upper bound of column key
     * @return a list of Columns
     */
    public List<Column<S,V>> query(K raw_key, S column_start, S column_end) {
        List<Column<S,V>> result = new ArrayList<>();
        if (!table.containsKey(raw_key)) {
            return result;
        }
        for (Map.Entry<S, V> entry:
                table.get(raw_key).subMap(column_start, true, column_end, true).entrySet()) {
            result.add(new Column<>(entry.getKey(), entry.getValue()));
        }
        return result;
    }
}