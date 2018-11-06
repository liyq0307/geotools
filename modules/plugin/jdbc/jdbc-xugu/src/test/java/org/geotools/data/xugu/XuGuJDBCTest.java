package org.geotools.data.xugu;

import java.sql.*;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * Created by liyq on 2018/10/19.
 */
public class XuGuJDBCTest {
    static private void closeSafe(ResultSet rs) {
        if (rs == null) {
            return;
        }

        try {
            rs.close();
        } catch (SQLException e) {
            String msg = "Error occurred closing result set";
            System.out.println(msg);
        }
    }

    public static void main(String args[]) {
        try {
            DriverManager.registerDriver(new com.xugu.cloudjdbc.Driver());
        } catch (SQLException e) {
            e.printStackTrace();
        }

        String url = "jdbc:xugu://localhost:5138/test?batch_mode=false";
        Properties properties = new Properties();
        properties.put("user", "SYSDBA");
        properties.put("password", "SYSDBA");
        properties.put("driver", "com.xugu.cloudjdbc.Driver");
        try {
            Connection conn = DriverManager.getConnection(url, properties);
            DatabaseMetaData metaData = conn.getMetaData();

            Set<String> availableTableTypes = new HashSet<String>();

            ResultSet tableTypes = metaData.getTableTypes();
            while (tableTypes.next()) {
                availableTableTypes.add(tableTypes.getString("TABLE_TYPE"));
            }
            closeSafe(tableTypes);

            ResultSet resultSet = metaData.getTables("test",
                    "public", "", availableTableTypes.toArray(new String[0]));
            while (resultSet.next()) {
                String schemaName = resultSet.getString("TABLE_SCHEM");
                String tableName = resultSet.getString("TABLE_NAME");
            }

//            Statement stm = conn.createStatement();
//            String sql = String.format("select * from dba_tables where upper(table_name) = upper(\'%s\')", "china_latest_RGrid");
//            ResultSet res = stm.executeQuery(sql);
//            while (res.next()) {
//                int i = 0;
//                i += 1;
//            }


        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
//            res.close();
//            stm.close();
//            conn.close();
        }
    }
}
