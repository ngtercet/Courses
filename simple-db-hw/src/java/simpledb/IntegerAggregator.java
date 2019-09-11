package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private LinkedHashMap<Field, Integer> map;
    private HashMap<Field, Integer> cnt;
    private TupleDesc td;
    private boolean grouping;


    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.map = new LinkedHashMap<>();
        this.grouping = gbfield != Aggregator.NO_GROUPING;
        if (what == Op.AVG) {
            this.cnt = new HashMap<>();
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field g = grouping ? tup.getField(gbfield) : null;
        int val = ((IntField) tup.getField(afield)).getValue();
        switch (what) {
            case MAX:
                max(g, val);
                break;
            case MIN:
                min(g, val);
                break;
            case SUM:
                sum(g, val);
                break;
            case COUNT:
                count(g, val);
                break;
            case AVG:
                avg(g, val);
                break;
            default:
                throw new RuntimeException("Should not reach here!");
        }
    }

    private void min(Field field, int val) {
        if (map.containsKey(field)) {
            if (map.get(field) > val) {
                map.put(field, val);
            }
        } else {
            map.put(field, val);
        }
    }

    private void max(Field field, int val) {
        if (map.containsKey(field)) {
            if (map.get(field) < val) {
                map.put(field, val);
            }
        } else {
            map.put(field, val);
        }
    }

    private void count(Field field, int val) {
        sum(field, 1);
    }

    private void avg(Field field, int val) {
        sum(field, val);
        if (cnt.containsKey(field)) {
            int oldVal = cnt.get(field);
            cnt.put(field, oldVal + 1);
        } else {
            cnt.put(field, 1);
        }
    }

    private void sum(Field field, int val) {
        if (map.containsKey(field)) {
            int oldVal = map.get(field);
            map.put(field, oldVal + val);
        } else {
            map.put(field, val);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     * if using group, or a single (aggregateVal) if no grouping. The
     * aggregateVal is determined by the type of aggregate specified in
     * the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        ArrayList<Tuple> tuples = new ArrayList<>();
        for (Field field : map.keySet()) {
            Tuple tuple = new Tuple(td);
            IntField aVal = what == Op.AVG
                    ? new IntField(map.get(field) / cnt.get(field))
                    : new IntField(map.get(field));
            if (field == null) {
                tuple.setField(0, aVal);
            } else {
                tuple.setField(0, field);
                tuple.setField(1, aVal);
            }
            tuples.add(tuple);
        }
        return new TupleIterator(td, tuples);
    }

    @Override
    public void setTupleDesc(TupleDesc td) {
        this.td = td;
    }

}
