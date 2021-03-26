package util;

import java.lang.reflect.Field;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class MyMySQLUtil {

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {

        }
    }

    /**
     * 从数据库执行sql 将resultset返回封装为clazz对应的类的对象
     *
     * @param sql
     * @param clazz
     * @param underScoreToCamel
     * @param <T>
     * @return
     */
    public static <T> List<T> queryList(String sql, Class<T> clazz, Boolean underScoreToCamel) {
        Connection conn = null;
        PreparedStatement pst = null;
        ResultSet rs = null;
        try {
            String url = "jdbc:mysql://hadoop102:3306/db_name?characterEncoding=utf-8&useSSL=false";
            conn = DriverManager.getConnection(
                    url,
                    "root",
                    "123456"
            );

            pst = conn.prepareStatement(sql);
            rs = pst.executeQuery();

            ResultSetMetaData md = rs.getMetaData();

            List<T> list = new ArrayList<>();

            while(rs.next()){
                T obj = clazz.newInstance();
                for (int i = 1; i <= md.getColumnCount(); i++) {
                    String colName = md.getColumnName(i);
                    Field colName1 = clazz.getField("colName");
                    colName1.set(obj,rs.getObject(i));
                }
                list.add(obj);
            }

            return list;
        } catch (Exception e) {
            throw new RuntimeException("123");
        } finally {
            if(conn != null){
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if(pst != null){
                try {
                    pst.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if(rs != null){
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) {

    }

}
