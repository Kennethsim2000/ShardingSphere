package org.example;

import com.zaxxer.hikari.HikariDataSource;
import io.github.cdimascio.dotenv.Dotenv;
import org.apache.shardingsphere.driver.api.ShardingSphereDataSourceFactory;
import org.apache.shardingsphere.infra.config.algorithm.AlgorithmConfiguration;
import org.apache.shardingsphere.sharding.api.config.ShardingRuleConfiguration;
import org.apache.shardingsphere.sharding.api.config.rule.ShardingTableRuleConfiguration;
import org.apache.shardingsphere.sharding.api.config.strategy.sharding.StandardShardingStrategyConfiguration;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ShardingApp {

    public static void main(String[] args) throws SQLException {
        Map<String, DataSource> dataSourceMap = new HashMap<>();
        dataSourceMap.put("ds_0", createDataSource("ds_0"));
        dataSourceMap.put("ds_1", createDataSource("ds_1"));

        ShardingRuleConfiguration shardingConfig = createShardingRule();
        //create shardingsphere datasource
        DataSource shardingDataSource = ShardingSphereDataSourceFactory.createDataSource(dataSourceMap,
                List.of(shardingConfig), new Properties());
        // takes in a map of string to datasource, as well as collection of RuleConfiguration,

        //insert data
        try(Connection conn = shardingDataSource.getConnection()) {
            for(int i = 1; i <= 4; i++) {
                PreparedStatement ps = conn.prepareStatement("INSERT INTO user (id,name) VALUES(?, ?)");
                ps.setLong(1, i);
                ps.setString(2, "USER_" + i);
                ps.executeUpdate();
                System.out.println("Inserted User_" + i);
            }
        }

        System.out.println("Done.");
    }

    private static ShardingRuleConfiguration createShardingRule() {
        ShardingRuleConfiguration shardingConfig = new ShardingRuleConfiguration();

        ShardingTableRuleConfiguration tableRuleConfiguration = new ShardingTableRuleConfiguration("user",
                "ds_${0..1}.user"); // used to define sharding rules for individual tables
        //used to specify how a logical table(user) is distributed across multiple physical tables

        tableRuleConfiguration.setDatabaseShardingStrategy(new
                StandardShardingStrategyConfiguration("id", "mod_sharding"));
        //Used to configure how data should be distributed across multiple databases

        //configure sharding algorithm
        shardingConfig.getTables().add(tableRuleConfiguration); // add in this ShardingTableRuleConfiguration

        Properties props = new Properties();
        props.setProperty("algorithm-expression", "ds_$->{id%2}");
        shardingConfig.getShardingAlgorithms().put("mod_sharding",new AlgorithmConfiguration("INLINE", props));
        //Insert our AlgorithmConfiguration

        //Inline expression is a piece of Groovy code in essence, which can return the corresponding real data
        // source or table name according to the computation method of sharding keys.
        return shardingConfig;
    }



    private static DataSource createDataSource(String dbName) {
        HikariDataSource ds = new HikariDataSource();
        ds.setDriverClassName("com.mysql.cj.jdbc.Driver"); // to use the mysql connector
        Dotenv dotenv = Dotenv.load();
        String JDBC_URL = dotenv.get("JDBC_URL");
        ds.setJdbcUrl(JDBC_URL + dbName + "?serverTimezone=UTC");
        String password = dotenv.get("DB_PASS");
        String username = dotenv.get("DB_USER");
        ds.setUsername(username);
        ds.setPassword(password);
        return ds;
    }
}
