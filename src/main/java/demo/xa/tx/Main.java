package demo.xa.tx;

import com.mysql.jdbc.jdbc2.optional.MysqlXAConnection;
import com.mysql.jdbc.jdbc2.optional.MysqlXid;

import javax.sql.XAConnection;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Random;

public class Main {

    public static void main(String[] args) throws ClassNotFoundException, SQLException, XAException {

        registerMySQlDriver();

        XAConnection xaConnection1 = getXAConnection();
        XAConnection xaConnection2 = getXAConnection();

        // 资源管理器
        XAResource rm1 = xaConnection1.getXAResource();
        XAResource rm2 = xaConnection2.getXAResource();

        // 全局事务ID
        String globalTxId = generateXAID();

        Xid xid1 = null;
        Xid xid2 = null;
        try {
            // 在全局事务内，对于每个XA事务，xid值的bqual部分应是不同的，该要求是对当前MySQL XA实施的限制。它不是XA规范的组成部分。
            String bqual1 = globalTxId + "-1";
            xid1 = new MysqlXid(globalTxId.getBytes(), bqual1.getBytes(), 1);

            // 执行 rm1 上的分支事务
            // @see http://jszx-jxpt.cuit.edu.cn/javaapi/javax/transaction/xa/XAResource.html
            rm1.start(xid1, XAResource.TMNOFLAGS);
            PreparedStatement ps1 = xaConnection1.getConnection().prepareStatement("insert into t(a,b) values (100,200)");
            ps1.execute();
            // 取消调用方与事务分支的关联。
            rm1.end(xid1, XAResource.TMSUCCESS);

            String bqual2 = globalTxId + "-2";
            xid2 = new MysqlXid(globalTxId.getBytes(), bqual2.getBytes(), 1);
            // 执行 rm2 上的分支事务
            rm2.start(xid2, XAResource.TMNOFLAGS);
            PreparedStatement ps2 = xaConnection2.getConnection().prepareStatement("insert into t(a,b) values (101,201)");
            ps2.execute();
            // 取消调用方与事务分支的关联。
            rm2.end(xid2, XAResource.TMSUCCESS);

            // 一阶段提交：通知所有资源管理器准备提交事务分支
            int rm1_prepared = rm1.prepare(xid1);
            int rm2_prepared = rm2.prepare(xid2);

            // 二阶段提交：所有事务分支都进入准备状态，提交所有事务分支
            // onePhase: true 代表使用单阶段协议提交，false 代表使用二阶段协议提交
            boolean onePhase = false;
            if (rm1_prepared == XAResource.XA_OK && rm2_prepared == XAResource.XA_OK) {
                rm1.commit(xid1, onePhase);
                rm2.commit(xid2, onePhase);
            } else {
                rm1.rollback(xid1);
                rm2.rollback(xid2);
            }

        } catch (XAException e) {
            rm1.rollback(xid1);
            rm2.rollback(xid2);
            e.printStackTrace();
        }
    }

    public static void registerMySQlDriver() throws ClassNotFoundException {
        Class.forName("com.mysql.jdbc.Driver");
    }

    public static XAConnection getXAConnection() throws SQLException {

        Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "123456");

        XAConnection xaConnection = new MysqlXAConnection((com.mysql.jdbc.Connection) connection, true);

        return xaConnection;
    }

    public static String generateXAID() {
        return "xa_" + (new Random()).nextInt(1000);
    }

}
