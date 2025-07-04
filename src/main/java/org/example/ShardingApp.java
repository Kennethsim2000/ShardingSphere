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

        testSharding(shardingDataSource);
        //insert data
        System.out.println("Done.");
    }

    private static void testSharding(DataSource source) {
        try(Connection conn = source.getConnection()) {
            createTablesIfNotExist(conn);
           insertTestData(conn);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    private static void insertTestData(Connection conn) throws SQLException {
        for(int i = 1; i <= 10; i++) {
            PreparedStatement ps = conn.prepareStatement("INSERT INTO user (id,name) VALUES(?, ?)");
            ps.setLong(1, i);
            ps.setString(2, "USER_" + i);
            ps.executeUpdate();
            System.out.println("Inserted User_" + i);
        }
    }

    private static void createTablesIfNotExist(Connection conn) throws SQLException {
        String createTableSQL = """
                -- ds_0.`user` definition
                
                CREATE TABLE `user` (
                  `id` bigint NOT NULL,
                  `name` varchar(50) DEFAULT NULL,
                  PRIMARY KEY (`id`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
                """;
        try(PreparedStatement statement = conn.prepareStatement(createTableSQL)) {
            statement.execute();
            System.out.println("Tables created successfully");
        }
    }


    private static ShardingRuleConfiguration createShardingRule() {
        ShardingRuleConfiguration shardingConfig = new ShardingRuleConfiguration();

        ShardingTableRuleConfiguration useTableRule = new ShardingTableRuleConfiguration("user",
                "ds_${0..1}.user_${0..1}"); // used to define sharding rules for individual tables
        //used to specify how a logical table(user) is distributed across multiple physical tables

        // database sharding strategy(shard by id % 2)
        //Used to configure how data should be distributed across multiple databases
        useTableRule.setDatabaseShardingStrategy(new
                StandardShardingStrategyConfiguration("id", "database_mod"));

        //table sharding strategy (shard by id % 2)
        useTableRule.setTableShardingStrategy(new StandardShardingStrategyConfiguration("id", "table_mod"));

        // Add table rule to sharding configuration
        shardingConfig.getTables().add(useTableRule); // add in this ShardingTableRuleConfiguration

        // Configure sharding algorithm
        Properties databaseProps = new Properties();
        databaseProps.setProperty("algorithm-expression", "ds_${id % 2}");
        shardingConfig.getShardingAlgorithms().put("database_mod",new AlgorithmConfiguration("INLINE", databaseProps));

        Properties tableProps = new Properties();
        tableProps.setProperty("algorithm-expression", "user_${id % 2}");
        shardingConfig.getShardingAlgorithms().put("table_mod", new AlgorithmConfiguration("INLINE", tableProps));
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

        //connection pool settings
        ds.setMaximumPoolSize(10);
        //Opening and closing a connection is an expensive operation, hence it helps us using the existing connections
        // from the pool whenever a connection is required. This sets the maximum number of connections that can be held in
        // the connection pool.
        ds.setMinimumIdle(5);
        //defines the minimum number of idle connections that the pool should try to maintain. An idle connection is one
        // that is not in use but is kept ready and in open state.
        ds.setConnectionTimeout(30000);
        //defines the maximum time application is willing to wait for a connection from the pool
        ds.setIdleTimeout(600000);
        //specifies the maximum amount of time a connection should remain idle before it closes. It helps in releasing
        // resources and prevents connection leaks.
        ds.setMaxLifetime(1800000);
        //sets the maximum lifetime for a connection in the pool, after this the connection is closed and is replaced
        // with the new connection in the pool.
        return ds;
    }
}
