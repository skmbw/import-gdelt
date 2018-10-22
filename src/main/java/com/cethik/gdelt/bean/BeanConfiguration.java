package com.cethik.gdelt.bean;

import com.cethik.geomesa.datastore.GeoMesaDataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Bean 配置。
 *
 * @author yinlei
 * @since 2018/10/22 11:19
 */
@Configuration
@EnableScheduling
public class BeanConfiguration {

    @Bean(destroyMethod = "dispose")
    public GeoMesaDataSource geoMesaDataSource() {
        GeoMesaDataSource geoMesaDataSource = new GeoMesaDataSource();
        geoMesaDataSource.init();
        return geoMesaDataSource;
    }

    // 在这里配置和在类定义处使用@Named配置是一样的，@SpringBootApplication(scanBasePackages="package")
//    @Bean
//    public GDELTScheduler gdeltScheduler() {
//        return new GDELTScheduler();
//    }
}
