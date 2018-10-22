package com.cethik.gdelt.task;

import com.cethik.geomesa.datastore.GeoMesaDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.scheduling.annotation.Scheduled;

import javax.inject.Inject;
import javax.inject.Named;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * 导入 GDELT 2.0 的任务调度器
 *
 * @author yinlei
 * @since 2018/10/22 11:13
 */
@Named
public class GDELTScheduler implements InitializingBean {
    private static final Logger LOGGER = LogManager.getLogger(GDELTScheduler.class);

    @Inject
    private GeoMesaDataSource geoMesaDataSource;

    @Override
    public void afterPropertiesSet() throws Exception {
        if (geoMesaDataSource == null) {
            throw new RuntimeException("property [geoMesaDataSource] cannot be null.");
        }
        // 初始化数据库表

    }

    @Scheduled(cron="0 25/2 * * * ?") // cron="0 5/15 * * * ?" // 每小时的5分钟开始，每15分钟执行一次
    public void schedule() {
        LOGGER.debug("现在时间是=[{}].", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now()));
    }
}
