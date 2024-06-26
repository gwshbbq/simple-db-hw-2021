package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;

/**
 * 迭代器操作，对统计好的分组groupresult，进行聚合操作，返回聚合后的resultSet的迭代器tupleIterator
 * **/
public class AggregateIter implements OpIterator {
    private Iterator<Tuple> resultSetIterator;
    private Map<Field, List<Field>> groupresult;//要迭代的数据
    private List<Tuple> resultSet;              //用来存储聚合后的数据
    private Aggregator.Op what;                 //聚合操作
    private TupleDesc tupleDesc;                //如果有group by就是两条，否则就是一条。

//    private int gbField;
//    private Type gbFieldType;

    public AggregateIter(Map<Field,List<Field>> groupresult,Aggregator.Op what,TupleDesc tupleDesc){
        this.groupresult = groupresult;
        this.what = what;
        this.tupleDesc = tupleDesc;
//        this.gbFieldType = gbFieldType;
//        this.gbField = gbField;
//        if(gbField!=-1){
//            Type[] type = new Type[2];
//            type[0] = gbFieldType;
//            type[1] = Type.INT_TYPE;
//            this.tupleDesc = new TupleDesc(type);
//        }else{
//            Type[] type = new Type[1];
//            type[0] = Type.INT_TYPE;
//            this.tupleDesc  = new TupleDesc(type);
//        }
    }


    @Override
    public void open() throws DbException, TransactionAbortedException {
        this.resultSet  = new ArrayList<>();
        if(what == Aggregator.Op.COUNT){
            for(Field field:groupresult.keySet()){
                Tuple tuple = new Tuple(this.tupleDesc);
                if(field!=null){ //分组
                    tuple.setField(0,field);
                    tuple.setField(1,new IntField(groupresult.get(field).size()));
                }else{
                    tuple.setField(0,new IntField(groupresult.get(field).size()));
                }
                this.resultSet.add(tuple);
            }
        }else if(what == Aggregator.Op.MIN){
            for(Field field:groupresult.keySet()){
                int min = Integer.MAX_VALUE;
                Tuple tuple = new Tuple(tupleDesc);
                for(int i=0;i<this.groupresult.get(field).size();i++){
                    IntField field1 = (IntField)groupresult.get(field).get(i);
                    min = Math.min(min,field1.getValue());
                }
                if(field!=null){
                    tuple.setField(0,field);
                    tuple.setField(1,new IntField(min));
                }else{
                    tuple.setField(0,new IntField(min));
                }
                resultSet.add(tuple);
            }
        }else if(what == Aggregator.Op.MAX){
            for(Field field:groupresult.keySet()){
                int max = Integer.MIN_VALUE;
                Tuple tuple = new Tuple(tupleDesc);
                for(int i=0;i<this.groupresult.get(field).size();i++){
                    IntField field1 = (IntField)groupresult.get(field).get(i);
                    if(field1.getValue()>max){
                        max = field1.getValue();
                    }
                }
                if(field!=null){
                    tuple.setField(0,field);
                    tuple.setField(1,new IntField(max));
                }else{
                    tuple.setField(0,new IntField(max));
                }
                resultSet.add(tuple);
            }
        }else if(what == Aggregator.Op.AVG){
            for(Field field: this.groupresult.keySet()){
                int sum = 0;
                int size = this.groupresult.get(field).size();
                Tuple tuple = new Tuple(tupleDesc);
                for(int i=0;i<size;i++){
                    IntField field1 = (IntField) groupresult.get(field).get(i);
                    sum += field1.getValue();
                }
                if(field!=null){
                    tuple.setField(0,field);
                    tuple.setField(1,new IntField(sum/size));
                }else{
                    tuple.setField(0,new IntField(sum/size));
                }
                resultSet.add(tuple);
            }
        }else if(what == Aggregator.Op.SUM){
            for(Field field:this.groupresult.keySet()){
                int sum = 0;
                Tuple tuple = new Tuple(tupleDesc);
                for(int i=0;i<this.groupresult.get(field).size();i++){
                    IntField field1  = (IntField) this.groupresult.get(field).get(i);
                    sum += field1.getValue();
                }
                if(field!=null){
                    tuple.setField(0,field);
                    tuple.setField(1,new IntField(sum));
                }else{
                    tuple.setField(0,new IntField(sum));
                }
                resultSet.add(tuple);
            }
        }
        this.resultSetIterator = resultSet.iterator();
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if(resultSetIterator==null){
            return false;
        }
        return resultSetIterator.hasNext();
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        return resultSetIterator.next();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        if(resultSet!=null){
            resultSetIterator = resultSet.iterator();
        }
    }

    @Override
    public TupleDesc getTupleDesc() {
        return this.tupleDesc;
    }

    @Override
    public void close() {
        this.resultSetIterator = null;
    }
}
