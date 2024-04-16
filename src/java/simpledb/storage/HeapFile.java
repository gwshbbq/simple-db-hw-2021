package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * 记住table(file)>page; bufferpool>page; page>tuple; tuple > field.
     * table(file)的page与bufferpool的page一样吗？
     * 不一样，table(file)其实没有page，只是数组在file中顺序存储，根据page大小可视为分为page，page大小字bufferpool中定义的
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    private File file;
    private TupleDesc tupleDesc;

    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return file.getAbsoluteFile().hashCode();//根据文件的绝对路径生成唯一id
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        HeapPage heapPage = null;
        int pageSize = BufferPool.getPageSize();
        byte[] buf = new byte[pageSize];

        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(this.file, "r");//选择文件
            randomAccessFile.seek((long)pid.getPageNumber()*pageSize);                 //选择读取的开始位置（页码*页的大小）
            if(randomAccessFile.read(buf)==-1){    //从指定位置读取数据到buf中，不论够不够会返回buf数组，
                                                    // 然后交给HeapPage封装成page对象；并把字节数据封装成Field对象
                                                    //这里是只读，没有改变磁盘数据。可以理解为把数据复制到page对象中。
                return null;                                         //如果没有页了才返回-1
            }
            heapPage= new HeapPage((HeapPageId) pid, buf);
            randomAccessFile.close();
        } catch (FileNotFoundException e ) {
            e.printStackTrace();
        } catch (IOException e){
            e.printStackTrace();
        }
        return heapPage;
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
        return (int)file.length()/BufferPool.getPageSize();
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
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
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid,Permissions.READ_ONLY);
    }

    public class HeapFileIterator implements DbFileIterator{
        TransactionId tid;
        Permissions permissions;
        BufferPool bufferPool =Database.getBufferPool();
        Iterator<Tuple> iterator;  //这个iterator是每一页的迭代器
        int num = 0;

        public HeapFileIterator(TransactionId tid,Permissions permissions){
            this.tid = tid;
            this.permissions = permissions;
        }

        /**
         * 开始进行遍历，默认从第一页开始
         * @throws DbException
         * @throws TransactionAbortedException
         */
        @Override
        public void open() throws DbException, TransactionAbortedException {
            num = 0;
            HeapPageId heapPageId = new HeapPageId(getId(), num);                       //先确定第一页的页id
            HeapPage page = (HeapPage)this.bufferPool.getPage(tid, heapPageId, permissions);
            if(page==null){
                throw  new DbException("page null");
            }else{
                iterator = page.iterator();
            }
        }

        /**
         * 获取下一有数据的页
         * @return
         * @throws DbException
         * @throws TransactionAbortedException
         */
        public boolean nextPage() throws DbException, TransactionAbortedException {
            while(true){
                num++;
                if(num>=numPages()){
                    return false;
                }
                HeapPageId heapPageId = new HeapPageId(getId(), num);
                HeapPage page = (HeapPage)bufferPool.getPage(tid,heapPageId,permissions);
                if(page==null){
                    continue;
                }
                iterator = page.iterator();
                if(iterator.hasNext()){
                    return true;
                }
            }
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if(iterator==null){
                return false;
            }
            if(iterator.hasNext()){
                return true;
            }else{
                return nextPage();
            }
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if(iterator==null){
                throw new NoSuchElementException();
            }
            return iterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            open();
        }

        @Override
        public void close() {
            iterator = null;
        }
    }
}

