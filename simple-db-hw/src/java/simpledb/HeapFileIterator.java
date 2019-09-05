package simpledb;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class HeapFileIterator extends AbstractDbFileIterator {
    private HeapFile hf;
    private Iterator<Tuple> it;
    private int curPgNo;
    private BufferPool bf;
    private TransactionId tid;

    public HeapFileIterator(HeapFile hf, TransactionId tid) {
        this.hf = hf;
        this.bf = Database.getBufferPool();
        this.tid = tid;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        curPgNo = 0;
        it = ((HeapPage) bf.getPage(tid, new HeapPageId(hf.getId(), curPgNo),
                Permissions.READ_ONLY)).iterator();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        this.curPgNo = 0;
        this.it = ((HeapPage) bf.getPage(tid, new HeapPageId(hf.getId(), curPgNo),
                Permissions.READ_ONLY)).iterator();
    }

    @Override
    public void close() {
        super.close();
        this.curPgNo = -1;
        this.bf = null;
        this.hf = null;
        this.it = null;
        this.tid = null;
    }

    @Override
    protected Tuple readNext() throws DbException, TransactionAbortedException {
        if (it == null) {
            return null;
        }
        if (it.hasNext()) {
            return it.next();
        } else if (curPgNo < hf.numPages() - 1) {
            curPgNo++;
            it = ((HeapPage) bf.getPage(tid, new HeapPageId(hf.getId(), curPgNo),
                    Permissions.READ_ONLY)).iterator();
            if (it.hasNext()) {
                return it.next();
            } else {
                return readNext();
            }
        } else {
            return null;
        }
    }
}
