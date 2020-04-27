package com.alipay.simplehbase.config;

import com.alipay.simplehbase.exception.SimpleHBaseException;
import com.alipay.simplehbase.util.ConfigUtil;
import com.alipay.simplehbase.util.StringUtil;
import com.alipay.simplehbase.util.TableNameUtil;
import com.alipay.simplehbase.util.Util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.log4j.Logger;
import org.springframework.core.io.Resource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * HbaseDataSource represent one hbase data source.
 *
 * @author xinzhi
 * */
public class HBaseDataSource {

    /** log. */
    final private static Logger log              = Logger.getLogger(HBaseDataSource.class);
    //----------config--------------
    /**
     * dataSource id.
     * */
    @ConfigAttr
    private String              id;
    /**
     * hbase's config resources, such as hbase zk config.
     * */
    @ConfigAttr
    private List<Resource>      hbaseConfigResources;

    //---------------------------runtime-------------------------
    /**
     * final hbase's config item.
     * */
    private Map<String, String> finalHbaseConfig = new HashMap<String, String>();

    /**
     * hbase Configuration.
     * */
    private Configuration       hbaseConfiguration;

    /**
     * init dataSource.
     * */
    public void init() {
        try {

            System.setProperty("javax.xml.parsers.DocumentBuilderFactory",
                    "com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl");
            System.setProperty("javax.xml.parsers.SAXParserFactory",
                    "com.sun.org.apache.xerces.internal.jaxp.SAXParserFactoryImpl");

            initHbaseConfiguration();

            log.info(this);

        } catch (Exception e) {
            log.error(e);
            throw new SimpleHBaseException(e);
        }
    }

    /**
     * Get Table by table Name.
     *
     * @param tableName tableName.
     * @return Table.
     * */
    public Table getHTable(String tableName) {
        Util.checkEmptyString(tableName);
        try {
            Connection connection = ConnectionFactory.createConnection(hbaseConfiguration);
            return connection.getTable(TableNameUtil.getTableName(tableName));
        } catch (Exception e) {
            log.error(e);
            throw new SimpleHBaseException(e);
        }
    }

    /**
     * Get one Admin.
     */
    public Admin getAdmin() {
        try {
            Connection connection = ConnectionFactory.createConnection(hbaseConfiguration);
            return connection.getAdmin();
        } catch (Exception e) {
            log.error(e);
            throw new SimpleHBaseException(e);
        }
    }

    /**
     * init HbaseConfiguration
     * */
    private void initHbaseConfiguration() {
        try {
            if (hbaseConfigResources != null) {
                for (Resource resource : hbaseConfigResources) {
                    finalHbaseConfig.putAll(ConfigUtil.loadConfigFile(resource
                            .getInputStream()));
                }
            }

            hbaseConfiguration = HBaseConfiguration.create();
            for (Map.Entry<String, String> entry : finalHbaseConfig.entrySet()) {
                hbaseConfiguration.set(entry.getKey(), entry.getValue());
            }

        } catch (Exception e) {
            log.error("parseConfig error.", e);
            throw new SimpleHBaseException("parseConfig error.", e);
        }
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Configuration getHbaseConfiguration() {
        return hbaseConfiguration;
    }

    public List<Resource> getHbaseConfigResources() {
        return hbaseConfigResources;
    }

    public void setHbaseConfigResources(List<Resource> hbaseConfigResources) {
        this.hbaseConfigResources = hbaseConfigResources;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("---------------datasource--------------------------\n");
        StringUtil.append(sb, "#id#", id);
        StringUtil.append(sb, "#finalHbaseConfig#", finalHbaseConfig);
        sb.append("---------------datasource--------------------------\n");
        return sb.toString();
    }

}
