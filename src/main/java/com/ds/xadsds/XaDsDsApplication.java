package com.ds.xadsds;

import org.h2.jdbcx.JdbcDataSource;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.XADataSourceWrapper;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.DataSourceInitializer;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import javax.sql.DataSource;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@SpringBootApplication
public class XaDsDsApplication {

    /*
    Необходим для обертки двух источников чтоб откатить обе транзакции в случае исключения
     */
    private final XADataSourceWrapper wrapper;

    public XaDsDsApplication(XADataSourceWrapper dataSourceWrapper) {
        this.wrapper = dataSourceWrapper;
    }

    public static void main(String[] args) {
        SpringApplication.run(XaDsDsApplication.class, args);
    }
    @Bean
    @ConfigurationProperties(prefix = "a")
    DataSource a() throws Exception {
        return this.wrapper.wrapDataSource(dataSource("a"));
    }

    @Bean
    @ConfigurationProperties(prefix = "b")
    DataSource b() throws Exception {
        return this.wrapper.wrapDataSource(dataSource("b"));
    }

    @Bean
    DataSourceInitializer aInit(DataSource a) {
        return init(a, "a");
    }

    @Bean
    DataSourceInitializer bInit(DataSource b) {
        return init(b , "b");
    }

    DataSourceInitializer init(DataSource a, String name) {
        DataSourceInitializer dsi = new DataSourceInitializer();
        dsi.setDataSource(a);
        dsi.setDatabasePopulator(new ResourceDatabasePopulator(new ClassPathResource(name + ".sql")));
        return dsi;
    }

    private JdbcDataSource dataSource(String dbName) {
        JdbcDataSource jdbcDataSource = new JdbcDataSource();
        jdbcDataSource.setURL("jdbc:h2:./" + dbName);
        jdbcDataSource.setUser("sa");
        jdbcDataSource.setPassword("");
        return jdbcDataSource;
    }

    @RestController

    public static class XaRestController {

        private final JdbcTemplate a;
        private final JdbcTemplate b;

        public XaRestController(DataSource a, DataSource b) {
            this.a = new JdbcTemplate(a);
            this.b = new JdbcTemplate(b);
        }

        @PostMapping
        @Transactional
        public void write(@RequestBody Map<String, String>payload, Optional<Boolean>rollback) {
            String name = payload.get("name");
            String msg = "Hello " + name + "!";
            this.a.update("insert into PET(id, nickname) values (?, ?)", UUID.randomUUID().toString(), name);
            this.b.update("insert into MESSAGE(id, message) values (?, ?)", UUID.randomUUID().toString(), msg);
            if (rollback.orElse(false)) {
                throw new RuntimeException("couldn'd write data to the database!");
            }
        }

        @GetMapping("/pets")
        public Collection<Map<String, String>>pets() {
            return this.a.query("select * from PET", (rs, rowNum) -> {
                Map<String, String>map = new HashMap<>();
                map.put("id", rs.getString("ID"));
                map.put("nickname", rs.getString("nickname"));
                return map;
            });
        }
        @GetMapping("/messages")
        public Collection<Map<String, String>>messages() {
            return this.b.query("select * from MESSAGE", (rs, rowNum) -> {
                Map<String, String>map = new HashMap<>();
                map.put("id", rs.getString("ID"));
                map.put("message", rs.getString("MESSAGE"));
                return map;
            });
        }
    }


}
