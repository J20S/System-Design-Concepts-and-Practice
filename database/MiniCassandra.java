import java.util.*;

/**
 * Cassandra is a NoSQL database, and it uses two-level keys to find a specific value.
 * In a Cassandra columnFamily (table in RDBMS), each row is determined by a raw_key (hash key / shard key).
 * Each row can have expandable number of columns, columns are sorted by column keys and support
 * range query. Column keys are usually ids or timestamp + id.
 * Created by Haitao (James) Li on 11/09/2016.
 */

public class MiniCassandra {

    private class Column {
        Object key;
        Object value;

        Column(Object key, Object value) {
            this.key = key;
            this.value = value;
        }

    }

    // we choose NavigableMap interface to support range query on column keys
    private Map<Object, NavigableMap<Object, Object>> columnFamily;

    public MiniCassandra() {
        columnFamily = new HashMap<>();
    }

    /**
     * @param raw_key hash key or shard key
     * @param column_key column key
     * @param column_value column value
     */
    public void insert(Object raw_key, Object column_key, Object column_value) {
        if (!columnFamily.containsKey(raw_key)) {
            columnFamily.put(raw_key, new TreeMap<>());
        }
        columnFamily.get(raw_key).put(column_key, column_value);
    }

    /**
     * Range query on column key
     * @param raw_key hash key or shard key
     * @param column_start lower bound of column key
     * @param column_end upper bound of column key
     * @return a list of Columns
     */
    public List<Column> query(Object raw_key, Object column_start, Object column_end) {
        List<Column> result = new ArrayList<>();
        if (!columnFamily.containsKey(raw_key)) {
            return result;
        }
        for (Map.Entry<Object, Object> entry:
                columnFamily.get(raw_key).subMap(column_start, true, column_end, true).entrySet()) {
            result.add(new Column(entry.getKey(), entry.getValue()));
        }
        return result;
    }
}