package simpledb.execution;

import simpledb.storage.Field;
import simpledb.storage.Tuple;

import java.io.Serializable;

/**
 * Predicate compares tuples to a specified Field value.
 * Field类型的字段与int类型的值比较
 * 比较元组某个特定的字段--> select * from t where t.a=1;    t.a=1
 * 只考虑如何比较，不考虑如何找到被比较的字段
 */
public class Predicate implements Serializable {

    private static final long serialVersionUID = 1L;
    private int field; //
    private Op op; //操作码
    private Field operand; //比较字段（IntField或StringField） t.a=1的1

    /** Constants used for return codes in Field.compare */
    public enum Op implements Serializable {
        //操作：=，>,<,>=,<=,like,<>
        EQUALS, GREATER_THAN, LESS_THAN, LESS_THAN_OR_EQ, GREATER_THAN_OR_EQ, LIKE, NOT_EQUALS;

        /**
         * Interface to access operations by integer value for command-line
         * convenience.
         * 
         * @param i
         *            a valid integer Op index
         */
        public static Op getOp(int i) {
            return values()[i];
        }

        public String toString() {
            if (this == EQUALS)
                return "=";
            if (this == GREATER_THAN)
                return ">";
            if (this == LESS_THAN)
                return "<";
            if (this == LESS_THAN_OR_EQ)
                return "<=";
            if (this == GREATER_THAN_OR_EQ)
                return ">=";
            if (this == LIKE)
                return "LIKE";
            if (this == NOT_EQUALS)
                return "<>";
            throw new IllegalStateException("impossible to reach here");
        }

    }
    
    /**
     * Constructor.
     * 
     * @param field
     *            field number of passed in tuples to compare against.
     * @param op
     *            operation to use for comparison
     * @param operand
     *            field value to compare passed in tuples to
     */

    public Predicate(int field, Op op, Field operand) {
        // some code goes here
        this.field = field;
        this.op  = op;
        this.operand = operand;
    }

    /**
     * @return the field number
     */
    public int getField()
    {
        // some code goes here
        return this.field;
    }

    /**
     * @return the operator
     */
    public Op getOp()
    {
        // some code goes here
        return this.op;
    }

    /**
     * @return the operand
     */
    public Field getOperand()
    {
        // some code goes here
        return this.operand;
    }
    
    /**
     * Compares the field number of t specified in the constructor to the
     * operand field specified in the constructor using the operator specific in
     * the constructor. The comparison can be made through Field's compare
     * method.
     * 
     * @param t
     *            The tuple to compare against
     * @return true if the comparison is true, false otherwise.
     * 利用IntField或者StringField的compare(Predicate.Op op, Field val)方法的返回结果
     */
    public boolean filter(Tuple t) {
        // some code goes here
        if (t == null) {
            return false;
        }
        Field f = t.getField(this.field);
        return f.compare(this.op, this.operand);

    }

    /**
     * Returns something useful, like "f = field_id op = op_string operand =
     * operand_string"
     */
    public String toString() {
        // some code goes here
         return "Predicate{"+
                "field="+this.field+
                "op="+this.op+
                "operand="+this.operand+
                "}";
    }
}
