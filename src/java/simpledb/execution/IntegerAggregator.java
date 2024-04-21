package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
     /**
     * 用于分组
     */
    private int gbfield;
    /**
     * 用于聚合
     */
    private int afield;
    private Type gbfieldtype;
    private Op what;
    /**
     * 存放结果
     */
    private Map<Field, List<Field>> groupresult;//需要迭代的数据，不是聚合完成的数据
    private TupleDesc tupleDesc;
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
//        this.group = new ConcurrentHashMap<>();
        this.groupresult = new HashMap<>();
        if(this.gbfield != NO_GROUPING){   //如果不分组,tupleDesc就是一条，否则就是两条。
            // 分组聚合,那么返回的聚合结果行由分组字段和该分组字段的聚合结果值组成
            this.tupleDesc = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE}, new String[]{"groupValue", "aggregateValue"});
        }else{
            this.tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"aggregateValue"});
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field af= tup.getField(this.afield);
        Field gb = null;;                 //如果不分组，groupresult里只有一个null为键的键值对
        if(this.gbfield != NO_GROUPING){   //如果不分组,tupleDesc就是一条，否则就是两条。
            gb = tup.getField(this.gbfield);
        }
        //按分组将聚合结果分类
        if(groupresult.containsKey(gb)){
            groupresult.get(gb).add(af);
        }else{
            List<Field> list = new ArrayList<>();
            list.add(af);
            groupresult.put(gb,list);
        }
    }

    public TupleDesc getTupleDesc() {
        return this.tupleDesc;
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     * 核心方法，迭代器操作，对统计好的分组groupresult，进行聚合操作，返回聚合后的resultSet的迭代器tupleIterator
     * 真正的聚合操作在迭代器初始化的时候以及聚合完成了
     */
    public OpIterator iterator() {
        // some code goes here
        return new AggregateIter(groupresult,what,tupleDesc);
    }

}
