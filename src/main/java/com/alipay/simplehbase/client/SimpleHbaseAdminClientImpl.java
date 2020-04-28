package com.alipay.simplehbase.client;

import com.alipay.simplehbase.config.HBaseDataSource;
import com.alipay.simplehbase.exception.SimpleHBaseException;
import com.alipay.simplehbase.util.TableNameUtil;
import com.alipay.simplehbase.util.Util;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.log4j.Logger;

/**
 * SimpleHbaseAdminClientImpl's implementation.
 *
 * @author xinzhi
 * */
public class SimpleHbaseAdminClientImpl implements SimpleHbaseAdminClient {

    /**
     * log.
     */
    private static Logger log = Logger.getLogger(SimpleHbaseAdminClientImpl.class);
    /**
     * HBaseDataSource.
     */
    private HBaseDataSource hbaseDataSource;

    @Override
    public void createTable(TableDescriptorBuilder tableDescriptorBuilder) {
        Util.checkNull(tableDescriptorBuilder);

        try {
            TableDescriptor build = tableDescriptorBuilder.build();
            Admin Admin = hbaseDataSource.getAdmin();
            NamespaceDescriptor[] namespaceDescriptors = Admin
                    .listNamespaceDescriptors();
            String namespace = build.getTableName()
                    .getNamespaceAsString();
            boolean isExist = false;
            for (NamespaceDescriptor nd : namespaceDescriptors) {
                if (nd.getName().equals(namespace)) {
                    isExist = true;
                    break;
                }
            }
            log.info("namespace " + namespace + " isExist " + isExist);
            if (!isExist) {
                Admin.createNamespace(NamespaceDescriptor
                        .create(namespace).build());
            }

            Admin.createTable(build);

            TableDescriptor newTableDescriptor = Admin.getDescriptor(build.getTableName());
            log.info("create table " + newTableDescriptor);
        } catch (Exception e) {
            log.error(e);
            throw new SimpleHBaseException(e);
        }
    }

    @Override
    public void deleteTable(String tableName) {
        Util.checkEmptyString(tableName);

        try {
            TableName tableName1 = TableNameUtil.getTableName(tableName);
            Admin Admin = hbaseDataSource.getAdmin();
            // delete table if table exist.
            if (Admin.tableExists(tableName1)) {
                // disable table before delete it.
                if (!Admin.isTableDisabled(tableName1)) {
                    Admin.disableTable(tableName1);
                }
                Admin.deleteTable(tableName1);
            }
        } catch (Exception e) {
            log.error(e);
            throw new SimpleHBaseException(e);
        }
    }

    @Override
    public HBaseDataSource getHbaseDataSource() {
        return this.hbaseDataSource;
    }

    @Override
    public void setHbaseDataSource(HBaseDataSource hbaseDataSource) {
        this.hbaseDataSource = hbaseDataSource;
    }

}
