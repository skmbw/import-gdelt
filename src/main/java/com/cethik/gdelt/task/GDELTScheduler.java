package com.cethik.gdelt.task;

import com.cethik.gdelt.utils.GDELTUtils;
import com.cethik.geomesa.datastore.GeoMesaDataSource;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.scheduling.annotation.Scheduled;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

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
        // 初始化数据库表，GDELT V2.0的全部字段和顺序
        String ecql = ""; // 可读性更好，而且编译器会在底层使用StringBuilder优化，没有性能损失

        // EVENTID AND DATE ATTRIBUTES
        ecql += "GlobalEventID:String,"; // 0
        ecql += "Date:Date,";
        ecql += "MonthYear:String,";
        ecql += "Year:String,";
        ecql += "FractionDate:Double,";

        // Actor1 ATTRIBUTES
        ecql += "Actor1Code:String,"; // 5
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
        ecql += "Actor2Code:String,"; // 15
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
        ecql += "IsRootEvent:Integer,"; // 24
        ecql += "EventCode:String:index=true,";
        ecql += "EventBaseCode:String,";
        ecql += "EventRootCode:String,";
        ecql += "QuadClass:Integer,";
        ecql += "GoldsteinScale:Double,";
        ecql += "NumMentions:Integer,";
        ecql += "NumSources:Integer,";
        ecql += "NumArticles:Integer,";
        ecql += "AvgTone:Double,"; // number

        // EVENT（Actor1） GEOGRAPHY
        ecql += "Actor1Geo_Type:Integer,"; // 35
        ecql += "Actor1Geo_Fullname:String,";
        ecql += "Actor1Geo_CountryCode:String,";
        ecql += "Actor1Geo_ADM1Code:String,";
        ecql += "Actor1Geo_ADM2Code:String,";
        ecql += "*Actor1Point:Point:srid=4326,"; // (40 41) 经纬度，用于索引
        ecql += "Actor1Geo_FeatureID:String,";

        // EVENT（Actor2） GEOGRAPHY
        ecql += "Actor2Geo_Type:Integer,"; // 43
        ecql += "Actor2Geo_Fullname:String,";
        ecql += "Actor2Geo_CountryCode:String,";
        ecql += "Actor2Geo_ADM1Code:String,";
        ecql += "Actor2Geo_ADM2Code:String,";
        ecql += "Actor2Point:Point:srid=4326,"; // (48 49)
        ecql += "Actor2Geo_FeatureID:String,";

        // EVENT（Action） GEOGRAPHY
        ecql += "ActionGeo_Type:Integer,"; // 51
        ecql += "ActionGeo_Fullname:String,";
        ecql += "ActionGeo_CountryCode:String,";
        ecql += "ActionGeo_ADM1Code:String,";
        ecql += "ActionGeo_ADM2Code:String,";
        ecql += "ActionPoint:Point:srid=4326,"; // (56 57)
        ecql += "ActionGeo_FeatureID:String,";

        // DATA MANAGEMENT FIELDS
        ecql += "DATEADDED:Integer,"; // 59
        ecql += "SOURCEURL:String";

        SimpleFeatureType featureType = SimpleFeatureTypes.createType("newgdelt", ecql);
        featureType.getUserData().put(SimpleFeatureTypes.DEFAULT_DATE_KEY, "Date");

//        geoMesaDataSource.deleteTable("newgdelt");
//        geoMesaDataSource.createTable(featureType);
    }

    @Scheduled(cron="0 2/15 * * * ?") // cron="0 5/15 * * * ?" // 每小时的5分钟开始，每15分钟执行一次
    public void schedule() {
        LOGGER.debug("现在时间是=[{}].", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now()));

        File file = new File("D:\\downloads\\20181022013000.export.CSV.zip");
        InputStream is = GDELTUtils.decompress(file);
        try {
            CSVParser parser = CSVParser.parse(is, StandardCharsets.UTF_8, CSVFormat.TDF);
            List<SimpleFeature> featureList = new ArrayList<>();
            for (CSVRecord record : parser) {
                LOGGER.debug("0=[{}]", record.get(0));
            }
        } catch (IOException e) {
            LOGGER.error("解析CSV文件错误。", e);
        }
    }
}
