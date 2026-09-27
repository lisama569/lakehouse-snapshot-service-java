package com.github.lisama569.lakehousesnapshotservicejava;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lisama569.lakehousesnapshotservicejava.config.LakehouseProperties;
import com.github.lisama569.lakehousesnapshotservicejava.engine.FaultControl;
import com.github.lisama569.lakehousesnapshotservicejava.engine.TableCatalog;
import com.github.lisama569.lakehousesnapshotservicejava.storage.LockManager;
import com.github.lisama569.lakehousesnapshotservicejava.storage.TableStateStore;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import java.nio.file.Path;

@SpringBootApplication
@EnableConfigurationProperties(LakehouseProperties.class)
public class Application {
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Bean
    public LockManager lockManager() {
        return new LockManager();
    }

    // 使用 Spring Boot 自动配置的 ObjectMapper（已注册 record 反序列化所需的
    // ParameterNamesModule 与 Jdk8/JavaTime 模块）。
    @Bean
    public TableStateStore tableStateStore(LakehouseProperties props,
                                          ObjectMapper mapper) {
        return new TableStateStore(Path.of(props.getDataDir()), mapper);
    }

    @Bean
    public TableCatalog tableCatalog(LakehouseProperties props,
                                     TableStateStore store,
                                     LockManager lockManager,
                                     ObjectMapper mapper,
                                     FaultControl faultControl) {
        return new TableCatalog(props, store, lockManager, mapper, faultControl);
    }
}
