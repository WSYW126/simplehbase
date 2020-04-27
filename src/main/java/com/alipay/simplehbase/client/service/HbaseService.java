package com.alipay.simplehbase.client.service;

import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Table;

/**
 * HbaseService
 *
 * @author xinzhi.zhang
 * */
public interface HbaseService {

    /**
     * Get a reference to the specified table.
     *
     * @param tableName table name
     * @return a reference to the specified table
     */
    public Table getTable(String tableName);

    /**
     * Get Admin.
     *
     * @return Admin
     * */
    public Admin getHBaseAdmin();
}
