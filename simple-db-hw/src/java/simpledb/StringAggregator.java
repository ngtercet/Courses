package simpledb;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private TupleDesc td;
    private LinkedHashMap<Field, Integer> map;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        if (what != Op.COUNT) {
            throw new IllegalArgumentException();
        }
        this.what = what;
        this.map = new LinkedHashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field g = gbfield == -1 ? null : tup.getField(gbfield);
        count(g);
    }

    private void count(Field field) {
        if (map.containsKey(field)) {
            int oldVal = map.get(field);
            map.put(field, oldVal + 1);
        } else {
            map.put(field, 1);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     * aggregateVal) if using group, or a single (aggregateVal) if no
     * grouping. The aggregateVal is determined by the type of
     * aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        ArrayList<Tuple> tuples = new ArrayList<>();
        for (Field field : map.keySet()) {
            Tuple tuple = new Tuple(td);
            IntField aVal = new IntField(map.get(field));
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