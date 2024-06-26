package simpledb.common;

import simpledb.common.Type;
import simpledb.storage.DbFile;
import simpledb.storage.HeapFile;
import simpledb.storage.TupleDesc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 * Catalog跟踪数据库中所有可用的表及其关联的模式。目前，这是一个存根目录，必须由用户程序用表填充，然后才能使用它
 * 最终，应该将其转换为从磁盘读取目录表的目录。
 * @Threadsafe
 */
public class Catalog {
     private List<Table> tables;
    /**
     * DbFile自定义接口是磁盘上数据库文件的接口。每个表都由一个 独立的DbFile表示
     * name就是表的名字；pkeyField就是主键的字段名称。**/
    public class Table {
        private DbFile file;
        private String name;
        private String pkeyField;

        public Table(DbFile file, String name, String pkeyField) {
            this.file = file;
            this.name = name;
            this.pkeyField = pkeyField;
        }

        public DbFile getFile() {
            return file;
        }

        public void setFile(DbFile file) {
            this.file = file;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getPkeyField() {
            return pkeyField;
        }

        public void setPkeyField(String pkeyField) {
            this.pkeyField = pkeyField;
        }

        @Override
        public String toString() {
            return "Mytable{" +
                    "file=" + file +
                    ", name='" + name + '\'' +
                    ", pkeyField='" + pkeyField + '\'' +
                    '}';
        }
    }
    /**
     * Constructor.
     * Creates a new, empty catalog.
     */

    public Catalog() {
        // some code goes here
        this.tables = new ArrayList<>();
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name the name of the table -- may be an empty string.  May not be null.  If a name
     * conflict exists, use the last table to be added as the table for a given name.
     * @param pkeyField the name of the primary key field
     */
    public void addTable(DbFile file, String name, String pkeyField) {
        // some code goes here
        Table table = new Table(file, name, pkeyField);
        for(int i=0;i<this.tables.size();i++){
            Table tmp = this.tables.get(i);
            if (tmp.getName()==null){
                continue;
            }
            if(tmp.getName().equals(name) || tmp.getFile().getId()==file.getId()){
                this.tables.set(i,table);
                return;
            }
        }
        this.tables.add(table);
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        // some code goes here
        if(name!=null){
            for(int i=0;i<this.tables.size();i++){
                if(this.tables.get(i).getName()==null){
                    continue;
                }
                if(this.tables.get(i).getName().equals(name)){
                    return this.tables.get(i).getFile().getId();
                }
            }
        }
        throw new NoSuchElementException();
    }
    /**
     * 通过tableId获取table
     * @param tableId
     * @return
     */
    public Table getTableById(int tableId){
        for(int i=0;i<this.tables.size();i++) {
            Table table = this.tables.get(i);
            if(table.getFile().getId()==tableId){
                return table;
            }
        }
        return null;
    }
    /**
     * Returns the tuple descriptor (schema) of the specified table
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        Table table = this.getTableById(tableid);
        if(table!=null){
            return table.getFile().getTupleDesc();
        }
        throw new NoSuchElementException();
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     */
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
        // some code goes here
        Table table = this.getTableById(tableid);
        if(table!=null){
            return table.getFile();
        }
        return null;

    }

    public String getPrimaryKey(int tableid) {
        // some code goes here
        Table table = this.getTableById(tableid);
        if(table!=null){
            return table.getPkeyField();
        }
        return null;
    }

    public Iterator<Integer> tableIdIterator() {
        // some code goes here
        List<Integer> tableList = new ArrayList<>();
        for(int i=0;i<this.tables.size();i++){
            tableList.add(this.tables.get(i).getFile().getId());
        }
        return tableList.iterator();
    }

    public String getTableName(int id) {
        // some code goes here
        Table table = this.getTableById(id);
        if(table!=null){
            return table.getName();
        }
        throw new NoSuchElementException();
    }
    
    /** Delete all tables from the catalog */
    public void clear() {
        // some code goes here
        this.tables.clear();
    }
    
    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder=new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(catalogFile));
            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<>();
                ArrayList<Type> types = new ArrayList<>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().equalsIgnoreCase("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().equalsIgnoreCase("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder+"/"+name + ".dat"), t);
                addTable(tabHf,name,primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println ("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}

