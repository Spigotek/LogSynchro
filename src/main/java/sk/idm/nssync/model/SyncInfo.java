package sk.idm.nssync.model;

public class SyncInfo {

    public String select;
    public String index;
    public String maping;
    public String table;
    public String timestampcol;
    public String datetimeformat;
    public String columntoelastic;
    public String idFromHash;
    public String hashcols;

    public SyncInfo(String key, String select, String index, String maping, String table, String timestampcol, String datetimeformat, String columntoelastic, String idFromHash, String hashcols) {
        this.select = select;
        this.index = index;
        this.maping = maping;
        this.table = table;
        this.timestampcol = timestampcol;
        this.datetimeformat = datetimeformat;
        this.columntoelastic = columntoelastic;
        this.idFromHash = (idFromHash == null)?"false":idFromHash;
        this.hashcols = hashcols;
    }
}
