package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 * 操作工具类，只支持单个列上的聚合，并按单个列分组。
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
     private OpIterator child;  //要操作的表的迭代器
    /**
     * 用于分组
     * **/
    private int gfield;
    private Type gbfieldtype;
    /**
     * 用于聚合
     * **/
    private int afield;
    private Aggregator.Op op;
    /**
     * 产生分组groupresult，和聚合resultset的迭代器
     * **/
    //根据字段的类型，您将需要构造一个IntegerAggregator或StringAggregator，
    private final Aggregator aggregator;
    //通过构造的IntegerAggregator或StringAggregator调用AggregateIter类来实现readNext()。
    private OpIterator resultSetIterator;   //迭代器
    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *根据字段的类型，您将需要构造一个IntegerAggregator或StringAggregator来实现readNext()。
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param op    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op op) {
        // some code goes here
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.op = op;
        Type fieldType = child.getTupleDesc().getFieldType(afield);
        if(gfield != -1){ //分组
            this.gbfieldtype = child.getTupleDesc().getFieldType(gfield);

        }else{                               //不分组，就一个
            this.gbfieldtype = null;
        }

        if(fieldType.equals(Type.INT_TYPE)){         //构造IntegerAggregator
                aggregator = new IntegerAggregator(gfield,gbfieldtype,afield,op);
        }else if(fieldType.equals(Type.STRING_TYPE)){ //构造StringAggregator
                aggregator = new StringAggregator(gfield,gbfieldtype,afield,op);
        }else{
                aggregator = null;
        }
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     * 返回分组字段索引
     */
    public int groupField() {
        // some code goes here
        if(gfield==-1){
            return Aggregator.NO_GROUPING;
        }
        return gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     * 返回分组字段名称
     */
    public String groupFieldName() {
        // some code goes here
        if(gfield==-1){
            return null;
        }
        try{
            return child.getTupleDesc().getFieldName(gfield);
        }catch (NoSuchElementException e){
            return null;
        }
    }

    /**
     * @return the aggregate field
     * 返回聚合字段索引
     */
    public int aggregateField() {
        // some code goes here
        return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     * 返回聚合字段名称
     */
    public String aggregateFieldName() {
        // some code goes here
        try{
            return child.getTupleDesc().getFieldName(afield);
        }catch (NoSuchElementException e){
            return null;
        }
    }

    /**
     * @return return the aggregate operator
     * 返回聚合操作
     */
    public Aggregator.Op aggregateOp() {
        // some code goes here
        return this.op;
    }

    /**
     * 返回聚合操作名称
     */
    public static String nameOfAggregatorOp(Aggregator.Op op) {
        return op.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        // some code goes here
        child.open();
        while(child.hasNext()){
            Tuple next = child.next();
            aggregator.mergeTupleIntoGroup(next); //将表的数据添加到分组数据groupresult中
        }
        this.resultSetIterator = aggregator.iterator();
        resultSetIterator.open();
        super.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(resultSetIterator!=null && resultSetIterator.hasNext()){
            return resultSetIterator.next();
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        resultSetIterator.rewind();
        child.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.aggregator.getTupleDesc();
    }

    public void close() {
        // some code goes here
        child.close();
        resultSetIterator.close();
        super.close();
    }
    //支持一个表的聚合分组操作
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
