package com.alipay.simplehbase.client;

import com.alipay.simplehbase.client.service.HBaseDataSourceAware;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;


/**
 * SimpleHbaseAdminClient.
 * 
 * @author xinzhi
 * */
public interface SimpleHbaseAdminClient extends HBaseDataSourceAware {

    /**
     * Creates a new table. Synchronous operation.
     */
    public void createTable(TableDescriptorBuilder tableDescriptorBuilder);

    /**
     * Deletes a table. Synchronous operation.
     */
    public void deleteTable(final String tableName);
}
