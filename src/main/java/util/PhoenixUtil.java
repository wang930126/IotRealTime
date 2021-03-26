package util;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class PhoenixUtil {

    private static Connection conn;
    private static String url = "jdbc:phoenix://hadoop102:2181";

    static {
        if (conn == null) {
            try {
                Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
                conn = DriverManager.getConnection(url);
                conn.setSchema("inspect_data_dw");
            } catch (Exception e) {
            }
        }
    }

    public static <T> List<T> queryList(String sql, Class<T> clazz) {
        PreparedStatement pst = null;
        List<T> rs = new ArrayList<T>();
        try {
            pst = conn.prepareStatement(sql);
            ResultSet resultSet = pst.executeQuery();
            ResultSetMetaData md = resultSet.getMetaData();
            while(resultSet.next()){
                for (int i = 1; i <= md.getColumnCount() ; i++) {
                    T instance = clazz.newInstance();
                    Object value = resultSet.getObject(i);
                    String columnName = md.getColumnName(i);
                    clazz.getField(columnName).set(instance,value);
                    rs.add(instance);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (pst != null) {
                try {
                    pst.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return rs;
    }
}
