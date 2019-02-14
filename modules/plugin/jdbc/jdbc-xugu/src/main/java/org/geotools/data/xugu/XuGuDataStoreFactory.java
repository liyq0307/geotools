package org.geotools.data.xugu;

import org.geotools.data.Parameter;
import org.geotools.jdbc.JDBCDataStore;
import org.geotools.jdbc.JDBCDataStoreFactory;
import org.geotools.jdbc.SQLDialect;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * Created by liyq on 2018/11/1.
 */
public class XuGuDataStoreFactory extends JDBCDataStoreFactory {
    /** parameter for database type */
    public static final Param DBTYPE =
            new Param(
                    "dbtype",
                    String.class,
                    "Type",
                    true,
                    "xugu",
                    Collections.singletonMap(Parameter.LEVEL, "program"));

    /** parameter for database port */
    public static final Param PORT = new Param("port", Integer.class, "Port", true, 5138);

    /** parameter for database schema */
    public static final Param SCHEMA = new Param("schema", String.class, "Schema", false, "SYSDBA");

    /** parameter for database parallel */
    public static final Param PARALLEL = new Param("parallel", Integer.class, "parallel", false, 4);

    @Override
    protected String getDatabaseID() {
        return (String)DBTYPE.sample;
    }

    @Override
    public String getDisplayName() {
        return "XuGu";
    }

    @Override
    protected String getDriverClassName() {
        return "com.xugu.cloudjdbc.Driver";
    }

    @Override
    protected SQLDialect createSQLDialect(JDBCDataStore jdbcDataStore) {
        return new XuGuDialect(jdbcDataStore);
    }

    @Override
    protected String getValidationQuery() {
        return null;
    }

    @Override
    public String getDescription() {
        return "XuGu Database";
    }

    @Override
    protected String getJDBCUrl(Map params) throws IOException {
        String host = (String) HOST.lookUp(params);
        String db = (String) DATABASE.lookUp(params);
        int port = (Integer) PORT.lookUp(params);
        return "jdbc:xugu" + "://" + host + ":" + port + "/" + db;
    }

    @Override
    protected void setupParameters(Map parameters) {
        super.setupParameters(parameters);
        parameters.put(DBTYPE.key, DBTYPE);
        parameters.put(SCHEMA.key, SCHEMA);
        parameters.put(PORT.key, PORT);
        parameters.put(PARALLEL.key, PARALLEL);
    }

    @Override
    protected JDBCDataStore createDataStoreInternal(JDBCDataStore dataStore, Map params)
            throws IOException {
        XuGuDialect dialect = (XuGuDialect)dataStore.getSQLDialect();
        Integer parallel = (Integer)PARALLEL.lookUp(params);
        dialect.setParallel(parallel == null ? 4 : parallel);

        return dataStore;
    }
}
