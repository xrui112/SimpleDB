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

    private File f;
    private TupleDesc td;


    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */

    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f=f;
        this.td=td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return f;
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
        int id=f.getAbsoluteFile().hashCode();
        return id;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        Page page = null;
        byte[] data = new byte[BufferPool.getPageSize()];

        try (RandomAccessFile raf = new RandomAccessFile(getFile(), "r")) {
            // page在HeapFile的偏移量
            int pos = pid.getPageNumber() * BufferPool.getPageSize();
            raf.seek(pos);
            raf.read(data, 0, data.length);
            page = new HeapPage((HeapPageId) pid, data);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return page;
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

        return (int) (f.length()/BufferPool.getPageSize());
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
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
    }
    private class HeapFileIterator implements DbFileIterator{
        private int PagePos;  //逻辑应该是 遍历不同的页数 再访问不同页数中的tuple

        private TransactionId tid;

        private HeapPage Page;

        private Iterator<Tuple> tupleIterator;
        public HeapFileIterator(TransactionId tid){
            this.tid=tid;
        }
        @Override
        public void open() throws DbException, TransactionAbortedException {
            PagePos=0;
            HeapPageId heapPageId=new HeapPageId(getId(),PagePos);
            //加载第一页的tuples
            Page= (HeapPage) Database.getBufferPool().getPage(tid,heapPageId,Permissions.READ_ONLY);
            tupleIterator=Page.iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if(Page==null){  //判断开始页是否为空
                throw new IllegalStateException("没有初始化或者初始页面为空");
            }
            if(tupleIterator.hasNext()){
               return true;
            }
            //程序运行到这里 表明这一页已经读完了 往后一页走
            if(PagePos < numPages()-1){
                PagePos++;
                HeapPageId heapPageId=new HeapPageId(getId(),PagePos);
                Page= (HeapPage) Database.getBufferPool().getPage(tid,heapPageId,Permissions.READ_ONLY); //得到后一页
                tupleIterator=Page.iterator();   //更新迭代器为后一页的迭代器
                return tupleIterator.hasNext();                                    //但是别着急 先看看这一页有没有值
            }else{ //表明没有下一页了
                return false;
            }
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if(!hasNext()){
                throw new IllegalStateException("该页无元素或者已经遍历完了");
            }
            return tupleIterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            //重置回到第一页
            open();
        }

        @Override
        public void close() {
            //清空状态 一切重置
            PagePos=0;
            Page=null;
            tupleIterator=null;
        }
    }

}

