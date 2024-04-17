package simpledb.execution;

import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 * 一个表的Tuple转化为一个iterator，然后筛选，通过predicate.filter()筛选
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;
     /**
     * 连接条件
     */
    private Predicate predicate;
    /**
     * 参与连接的表
     */
    private OpIterator child;  //一个表的时候只把一个表的Tuple转化为一个iterator，两个表的时候两个iterator，chirdren[0],chirdren[1]
    private final List<Tuple> childTuple = new ArrayList<>(); //保持通过predicate.filter()筛选的Tuple
    private Iterator<Tuple> it;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        // some code goes here
        this.predicate = p;
        this.child = child;
    }

    public Predicate getPredicate() {
        // some code goes here
        return this.predicate;
    }
    //迭代器都是一个表的迭代，迭代的是Tuple
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.child.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        this.child.open();//去掉可以吗？有什么用？
        while(this.child.hasNext()){
            Tuple next = this.child.next();
            if(this.predicate.filter(next)){
                this.childTuple.add(next);
            }
        }
        it = childTuple.iterator();
        super.open();
    }

    public void close() {
        // some code goes
        this.child.close(); //去掉可以吗？有什么用？
        it = null;
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        it = childTuple.iterator();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes
//        while (this.child.hasNext()) {
//            Tuple tuple = this.child.next();
//            if (this.predicate.filter(tuple)) {
//                return tuple;
//            }
//        }
        if(it != null && it.hasNext() ){
            return it.next();
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child = children[0];
    }

}
