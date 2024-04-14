package simpledb.storage;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 自己实现的一个LRU cache,主要就是map+链表实现
 */
public class LRUCache<K,V> {
    class DLinkedNode{
        K key;
        V value;
        DLinkedNode prev;
        DLinkedNode next;
        public DLinkedNode() {}
        public DLinkedNode(K _key, V _value) {
            this.key = _key;
            this.value = _value;
        }
    }
    private Map<K,DLinkedNode> cache = new ConcurrentHashMap<K,DLinkedNode>();
    private int size;
    private int capacity;
    private DLinkedNode head,tail;
    public LRUCache(int capacity){
        this.capacity = capacity;
        this.size = 0;
        this.head = new DLinkedNode();
        this.tail = new DLinkedNode();
        this.head.next = this.tail;
        this.head.prev = this.tail;
        this.tail.prev = this.head;
        this.tail.next = this.head;
//            head.next = tail;
//            head.prev = tail;
//            tail.prev = head;
//            tail.next = head;
    }

    public int getSize() {
        return this.size;
    }
    public DLinkedNode getHead(){
        return this.head;
    }
    public DLinkedNode getTail(){
        return this.tail;
    }
    public Map<K,DLinkedNode> getCache(){
        return this.cache;
    }
    /**
     * 根据key获取元素,有锁
     */
     public synchronized V get(K key){
        DLinkedNode node = cache.get(key);
        if(node==null){
            return null;
        }
        //如果存在，移到头部
        moveToHead(node);
        return node.value;
    }

    /**
     * 新增元素，注意容量限制，有锁
     */
    public synchronized void put(K key,V value){
        DLinkedNode node = this.cache.get(key);
        if(node==null){
            //新增
            DLinkedNode newNode = new DLinkedNode(key, value);
            this.cache.put(key, newNode);
            addToHead(newNode);
            this.size++;
            //判断是否达到上限
            if(this.size>this.capacity){
                //删掉最后一个元素
                DLinkedNode tmp = tail.prev;
                //先在链表中移除
                removeNode(tmp);
                //然后在map中移除
                this.cache.remove(tmp.key);
                this.size--;
            }
        }else{
            //修改
            node.value = value;
            moveToHead(node);
        }
    }

    //移动节点
    public void moveToHead(DLinkedNode node){
        removeNode(node);
        addToHead(node);
    }
    public void removeNode(DLinkedNode node){
        node.prev.next = node.next;
        node.next.prev = node.prev;
    }
    public void addToHead(DLinkedNode node){
        node.prev = head;
        node.next = head.next;
        head.next.prev = node;
        head.next = node;
    }

    //彻底删除节点
//    public void remove(DLinkedNode node){
//        removeNode(node);
//        cache.remove(node.key);
//        size--;
//    }

//    public synchronized void discard(){
//        // 如果超出容量，删除双向链表的尾部节点
//        DLinkedNode tail = removeTail();
//        // 删除哈希表中对应的项
//        cache.remove(tail.key);
//        size--;
//    }
//    private DLinkedNode removeTail() {
//        DLinkedNode res = tail.prev;
//        removeNode(res);
//        return res;
//    }
}