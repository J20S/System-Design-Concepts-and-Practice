import java.util.HashMap;
import java.util.Map;

/**
 * Memcache is a useful technique to cache small chunks of data from database, API, page rendering and etc to
 * boost up entire web application performance.
 *
 * Memcache is essentially a collection of key-value pairs. The capacity of the map depends on how big the
 * memory is. When the memory is full and new key-value pair is to be added, we need to remove some old key-value
 * pair to clea up some space. There are two classic approaches: LRU cache (remove least recently used key-value pair)
 * and LFU cache (remove least frequently used key-value pair). The choice of a particular approach depends on the
 * use case. In this implementation, we will use LRU cache.
 *
 * It can be a good practice to set up expiration times for stored key-value pair. Cache can become out of sync when
 * your app has a bug, a crash, a network blip or some other issue. We only want one source of truth.
 *
 * Created by Haitao (James) Li on 26/09/2016.
 */

public class Memcache {

    /**
     * A wrapper class used to store relevant information about a particular key-value pair.
     * More importantly, it used LinkedList implementation to keep track of the insertion order, which
     * makes it easy for us to delete least recently used nodes.
     */
    private class Node {
        Node prev;
        Node next;
        Object key; // Note we specifically store key here so that it makes it possible for us to delete LRU node
        Object data;
        int timestamp;
        int ttl;
        Node(Object key, Object data, int timestamp, int ttl) {
            this.key = key;
            this.data = data;
            this.timestamp = timestamp;
            this.ttl = ttl;
        }

        boolean isValid(int curTime) {
            return ttl == 0 || timestamp + ttl > curTime;
        }
    }

    private Map<Object, Node> storage;
    // Dummy nodes to make it easier for us to handle edge cases
    private Node head, tail;
    private int capacity;

    public Memcache(int capacity) {
        this.capacity = capacity;
        storage = new HashMap<>(capacity);
        // Initialise head and tail dummy nodes, any newly added nodes will be like head -> new ... -> tail
        // When map is full, we remove the next node to head as it the least recent
        head = new Node(null, null, 0, 0);
        tail = new Node(null, null, 0, 0);
        head.next = tail;
        tail.prev = head;
    }

    /**
     * Set a key value pair.
     * If key already exists, we modify the value and move the node right before dummy tail (most recent node).
     * Otherwise we create a new node and put it right before dummy tail.
     * @param key key
     * @param value value
     * @param curTime current time
     * @param ttl time to leave (expire)
     */
    public void set(Object key, Object value, int curTime, int ttl) {
        Node node = storage.get(key);
        if (node == null) {
            if (storage.size() == capacity) {
                delete(head.next.key);
            }
            node = new Node(key, value, curTime, ttl);
            storage.put(key, node);
        }
        else {
            // move prev and next pointers
            node.prev.next = node.next;
            node.next.prev = node.prev;
            node.timestamp = curTime;
            node.ttl = ttl;
            node.data = value;
        }
        moveToTail(node);
    }

    /**
     * Get value based on key, if key exists and data hasn't expired, we move the node right before dummy tail
     * as the most recent node.
     * @param key key
     * @param curTime current time
     * @return value if ket exists and data hasn't expired, null otherwise
     */
    public Object get(Object key, int curTime) {
        Node node = storage.get(key);
        if (node == null || !node.isValid(curTime)) {
            return null;
        }
        node.timestamp = curTime;
        // move prev and next pointers
        node.prev.next = node.next;
        node.next.prev = node.prev;
        moveToTail(node);
        return node.data;
    }

    /**
     * Delete the node if key exists, move prev and next pointers as well for the deleted node.
     * @param key key
     */
    public void delete(Object key) {
        if (storage.containsKey(key)) {
            Node node = storage.get(key);
            storage.remove(key);
            node.prev.next = node.next;
            node.next.prev = node.prev;
        }
    }

    /**
     * Move given node right before dummy tail as the most recent node.
     * @param node node
     */
    private void moveToTail(Node node) {
        node.prev = tail.prev;
        tail.prev.next = node;
        node.next = tail;
        tail.prev = node;
    }

    /**
     * @return size of the current storage map.
     */
    public int size() {
        return storage.size();
    }
}
