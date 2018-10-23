package com.cethik.gdelt.task;

import com.cethik.geomesa.datastore.GeoMesaDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes;
import org.opengis.feature.simple.SimpleFeatureType;
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
        String ecql = ""; // 可读性更好，而且编译器会在底层使用StringBuilder优化，没有性能损失

        // EVENTID AND DATE ATTRIBUTES
        ecql += "GlobalEventID:String,";
        ecql += "Date:Date,";
        ecql += "MonthYear:String,";
        ecql += "Year:String,";
        ecql += "FractionDate:Double,";

        // Actor1 ATTRIBUTES
        ecql += "Actor1Code:String,";
        ecql += "Actor1Name:String,";
        ecql += "Actor1CountryCode:String,";
        ecql += "Actor1KnownGroupCode:String,";
        ecql += "Actor1EthnicCode:String,";
        ecql += "Actor1Religion1Code:String,";
        ecql += "Actor1Religion2Code:String,";
        ecql += "Actor1Type1Code:String,";
        ecql += "Actor1Type2Code:String,";
        ecql += "Actor1Type3Code:String,";

        // Actor2 ATTRIBUTES
        ecql += "Actor2Code:String,";
        ecql += "Actor2Name:String,";
        ecql += "Actor2CountryCode:String,";
        ecql += "Actor2KnownGroupCode:String,";
        ecql += "Actor2EthnicCode:String,";
        ecql += "Actor2Religion1Code:String,";
        ecql += "Actor2Religion2Code:String,";
        ecql += "Actor2Type1Code:String,";
        ecql += "Actor2Type2Code:String,";
        ecql += "Actor2Type3Code:String,";

        // EVENT ACTION ATTRIBUTES
        ecql += "IsRootEvent:Integer,";
        ecql += "EventCode:String:index=true,";
        ecql += "EventBaseCode:String,";
        ecql += "EventRootCode:String,";
        ecql += "QuadClass:Integer,";
        ecql += "GoldsteinScale:Double,";
        ecql += "NumMentions:Integer,";
        ecql += "NumSources:Integer,";
        ecql += "AvgTone:Integer,"; // number

        // EVENT（Actor1） GEOGRAPHY
        ecql += "Actor1Geo_Type:Integer,";
        ecql += "Actor1Geo_Fullname:String,";
        ecql += "Actor1Geo_CountryCode:String,";
        ecql += "Actor1Geo_ADM1Code:String,";
        ecql += "Actor1Geo_ADM2Code:String,";
        ecql += "*Actor1Point:Point:srid=4326,"; // 经纬度，用于索引
        ecql += "Actor1Geo_FeatureID:String,";

        // EVENT（Actor2） GEOGRAPHY
        ecql += "Actor2Geo_Type:Integer,";
        ecql += "Actor2Geo_Fullname:String,";
        ecql += "Actor2Geo_CountryCode:String,";
        ecql += "Actor2Geo_ADM1Code:String,";
        ecql += "Actor2Geo_ADM2Code:String,";
        ecql += "Actor2Point:Point:srid=4326,";
        ecql += "Actor2Geo_FeatureID:String,";

        // EVENT（Action） GEOGRAPHY
        ecql += "ActionGeo_Type:Integer,";
        ecql += "ActionGeo_Fullname:String,";
        ecql += "ActionGeo_CountryCode:String,";
        ecql += "ActionGeo_ADM1Code:String,";
        ecql += "ActionGeo_ADM2Code:String,";
        ecql += "ActionPoint:Point:srid=4326,";
        ecql += "ActionGeo_FeatureID:String,";

        // DATA MANAGEMENT FIELDS
        ecql += "DATEADDED:Integer,";
        ecql += "SOURCEURL:String,";

        SimpleFeatureType featureType = SimpleFeatureTypes.createType("newgdelt", ecql);
        featureType.getUserData().put(SimpleFeatureTypes.DEFAULT_DATE_KEY, "Date");

        geoMesaDataSource.createTable(featureType);
    }

    @Scheduled(cron="0 25/2 * * * ?") // cron="0 5/15 * * * ?" // 每小时的5分钟开始，每15分钟执行一次
    public void schedule() {
        LOGGER.debug("现在时间是=[{}].", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now()));
    }
}
