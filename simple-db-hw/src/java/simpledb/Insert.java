package simpledb;

import java.io.IOException;
import java.util.Arrays;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private OpIterator child;
    private int tableId;
    private TupleDesc td;
    private int affected;
    private boolean called;

    /**
     * Constructor.
     *
     * @param tid     The transaction running the insert.
     * @param child   The child operator from which to read tuples to be inserted.
     * @param tableId The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to
     *                     insert.
     */
    public Insert(TransactionId tid, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        this.tid = tid;
        this.child = child;
        setChildren(new OpIterator[]{child});
        this.tableId = tableId;
        this.td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"Inserted"});
        TupleDesc tableTD = Database.getCatalog().getTupleDesc(tableId);
        if (!child.getTupleDesc().equals(tableTD)) {
            throw new DbException("TupleDesc of child differs from table into which we are to insert");
        }
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        child.open();
    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
        called = false;
        affected = 0;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
        affected = 0;
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     * null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (called) {
            return null;
        }
        BufferPool bp = Database.getBufferPool();
        while (child.hasNext()) {
            Tuple tuple = child.next();
            try {
                bp.insertTuple(tid, tableId, tuple);
                affected += 1;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        Tuple ret = new Tuple(td);
        Field val = new IntField(affected);
        ret.setField(0, val);
        called = true;
        return ret;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return (OpIterator[]) children.toArray();
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.children.addAll(Arrays.asList(children));
    }
}
