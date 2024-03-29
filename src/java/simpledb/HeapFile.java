package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File heapFile;
    private TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        heapFile = f;
        tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return heapFile;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return heapFile.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        try {
            int position = Database.getBufferPool().getPageSize()*pid.pageNumber();
            FileInputStream fileInputStream = null;
            byte[] bFile = new byte[(int) heapFile.length()];
            fileInputStream = new FileInputStream(heapFile);
            fileInputStream.read(bFile);
            fileInputStream.close();
            byte[] data = Arrays.copyOfRange(bFile, position, position + Database.getBufferPool().getPageSize());
            HeapPageId heapPageId = (HeapPageId) pid;
            return new HeapPage(heapPageId, data);
        } catch (IOException e) {
            return null;
        } catch (NoSuchElementException e) {
            return null;
        }
        
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) Math.ceil(this.heapFile.length()/Database.getBufferPool().getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid){
        // some code goes here
        return new HeapFileIterator(this, tid);
    }

    private class HeapFileIterator implements DbFileIterator {

        private HeapFile hfile;
        private int numPages;
        private TransactionId tid;
        private int currentPageNum;
        private Iterator<Tuple> currentIter;
        private boolean open;

        
        public HeapFileIterator(HeapFile hfile, TransactionId tid){
            this.hfile = hfile;
            this.numPages = hfile.numPages();
            this.tid = tid;
            this.currentPageNum = 0;
            this.currentIter = getPageIterator(currentPageNum);
            this.open = false;          

        }

        public void open(){
            open = true;
        }

        public boolean hasNext(){
            if (open) {
                return currentPageNum < numPages;
            }
            else {
                return false;
            }
        }


        public Tuple next() {
            if (hasNext()){
                if (currentIter.hasNext()) {
                    Tuple result = currentIter.next();
                    if (!currentIter.hasNext()){
                        currentPageNum++;
                        currentIter = getPageIterator(currentPageNum);
                    }
                    return result;
                } 
            } else {
                throw new NoSuchElementException();
            }
            return null;
        }

        public void rewind(){

        }

        public void close(){
            open = false;
        }

        private Iterator<Tuple> getPageIterator(int pageNum){
            
            HeapPageId heapPageId = new HeapPageId(hfile.getId(), pageNum);
            //HeapPage currentPage = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_ONLY);
            HeapPage currentPage = (HeapPage) hfile.readPage(heapPageId);
            Iterator<Tuple> iter = currentPage.iterator();
            return iter;
           
           
        }
    }

}

