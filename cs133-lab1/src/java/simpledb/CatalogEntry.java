package simpledb;

public class CatalogEntry {
    private final int id;
    private final DbFile file;
    private final String name;
    private final String pkeyField;

    public CatalogEntry(int id, DbFile file, String name, String pkeyField) {
        this.id = id;
        this.file = file;
        this.name = name;
        this.pkeyField = pkeyField;
    }

    // 添加相应的 getter 方法
    public int getId() {
        return id;
    }

    public DbFile getFile() {
        return file;
    }

    public String getName() {
        return name;
    }

    public String getPkeyField() {
        return pkeyField;
    }
}
