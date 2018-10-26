package com.cethik.gdelt.task;

import com.cethik.gdelt.utils.GDELTUtils;
import com.cethik.geomesa.datastore.GeoMesaDataSource;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.geotools.factory.Hints;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.scheduling.annotation.Scheduled;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;

/**
 * 导入 GDELT 2.0 的任务调度器
 *
 * @author yinlei
 * @since 2018/10/22 11:13
 */
@Named
public class GDELTScheduler implements InitializingBean {
    private static final Logger LOGGER = LogManager.getLogger(GDELTScheduler.class);

    private static final DateTimeFormatter FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd", Locale.US);
    private static final DateTimeFormatter FORMAT_FULL = DateTimeFormatter.ofPattern("yyyyMMddHHmmss", Locale.US);

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
        ecql += "GlobalEventID:String,"; // record index is 0, column index is 0
        ecql += "Date:Date,";
        ecql += "MonthYear:String,";
        ecql += "Year:String,";
        ecql += "FractionDate:Double,"; // 4

        // Actor1 ATTRIBUTES
        ecql += "Actor1Code:String,"; // record index is 5, column index is 0
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
        ecql += "IsRootEvent:Integer,"; // 25, 25
        ecql += "EventCode:String:index=true,";
        ecql += "EventBaseCode:String,";
        ecql += "EventRootCode:String,";
        ecql += "QuadClass:Integer,"; // 29
        ecql += "GoldsteinScale:Double,"; // 30
        ecql += "NumMentions:Integer,"; // 31
        ecql += "NumSources:Integer,"; // 32
        ecql += "NumArticles:Integer,"; // 33
        ecql += "AvgTone:Double,"; // number 34

        // EVENT（Actor1） GEOGRAPHY
        ecql += "Actor1Geo_Type:Integer,"; // 35, 35
        ecql += "Actor1Geo_Fullname:String,";
        ecql += "Actor1Geo_CountryCode:String,";
        ecql += "Actor1Geo_ADM1Code:String,";
        ecql += "Actor1Geo_ADM2Code:String,";
        ecql += "*Actor1Point:Point:srid=4326,"; // (40 41) 经纬度，用于索引, column index is 40
        ecql += "Actor1Geo_FeatureID:String,"; // record index is 42, column index is 41

        // EVENT（Actor2） GEOGRAPHY
        ecql += "Actor2Geo_Type:Integer,"; // record index is 43, column index is 42
        ecql += "Actor2Geo_Fullname:String,";
        ecql += "Actor2Geo_CountryCode:String,";
        ecql += "Actor2Geo_ADM1Code:String,";
        ecql += "Actor2Geo_ADM2Code:String,";
        ecql += "Actor2Point:Point:srid=4326,"; // (48 49), column index is 47
        ecql += "Actor2Geo_FeatureID:String,";

        // EVENT（Action） GEOGRAPHY
        ecql += "ActionGeo_Type:Integer,"; // 51, 49
        ecql += "ActionGeo_Fullname:String,";
        ecql += "ActionGeo_CountryCode:String,";
        ecql += "ActionGeo_ADM1Code:String,";
        ecql += "ActionGeo_ADM2Code:String,";
        ecql += "ActionPoint:Point:srid=4326,"; // (56 57), column index is 54
        ecql += "ActionGeo_FeatureID:String,";

        // DATA MANAGEMENT FIELDS
        ecql += "DATEADDED:String,"; // 59, 56，YYYYMMDDHHMMSS
        ecql += "SOURCEURL:String";// 60, 57

        SimpleFeatureType featureType = SimpleFeatureTypes.createType("newgdelt", ecql);
        featureType.getUserData().put(SimpleFeatureTypes.DEFAULT_DATE_KEY, "Date");

//        geoMesaDataSource.deleteTable("newgdelt");
//        geoMesaDataSource.createTable(featureType);

        LOGGER.info("Start GDELT V2.0 Importer Scheduler Success.");
    }

    @Scheduled(cron="0 50 10 * * ?") // cron="0 5/15 * * * ?" // 每小时的5分钟开始，每15分钟执行一次
    public void schedule() {
        LOGGER.debug("现在时间是=[{}].", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now()));

        // 2018-1-1
        LocalDateTime start = LocalDateTime.of(2018, 1, 1, 0, 0, 0);
        // 2018-10-25
        LocalDateTime end = LocalDateTime.of(2018,10, 25, 23, 59, 59);

        for (; start.isBefore(end) ; start = start.plusMinutes(15)) {
            String filePath = start.format(FORMAT_FULL) + ".export.CSV.zip";
            LOGGER.info("处理文件：[" + filePath + "]开始...");
            try {
                byte[] resultArray = GDELTUtils.get("http://data.gdeltproject.org/gdeltv2/" + filePath);
                byte[] csvByteArray = GDELTUtils.unzip(resultArray);
                if (csvByteArray == null || csvByteArray.length == 0) {
                    continue;
                }
                ByteArrayInputStream is = new ByteArrayInputStream(csvByteArray);

                SimpleFeatureBuilder builder = new SimpleFeatureBuilder(geoMesaDataSource.getSimpleFeatureType("newgdelt"));
                CSVParser parser = CSVParser.parse(is, StandardCharsets.UTF_8, CSVFormat.TDF);
                List<SimpleFeature> featureList = new ArrayList<>();
                for (CSVRecord record : parser) {
                    SimpleFeature feature = buildFeature(record, builder);
                    featureList.add(feature);
                }
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("数据size=[{}].", featureList.size());
                }
                geoMesaDataSource.batchInsert("newgdelt", featureList);
            } catch (Exception e) {
                LOGGER.error("解析CSV文件错误。", e);
            }
        }
    }

    private SimpleFeature buildFeature(CSVRecord record, SimpleFeatureBuilder builder) throws ParseException {
        String globalEventId = record.get(0);
        // 及时使用字段，底层还是使用字段查询出索引，然后使用索引，so直接使用索引
        builder.set(0, globalEventId);
        builder.set(1, Date.from(LocalDate.parse(record.get(1), FORMAT).atStartOfDay(ZoneOffset.UTC).toInstant()));
        for (int i = 2, j = 0; i <= 60; i++) {
            switch (i) {
                case 25:
                case 29:
                case 31:
                case 32:
                case 33:
                case 35:
                case 43:
                case 51:
                    // Integer
                    builder.set(i - j, Integer.valueOf(record.get(i)));
                    break;
                case 4:
                case 30:
                case 34:
                    // Double
                    builder.set(i - j, Double.valueOf(record.get(i)));
                    break;
                case 40:
                case 48:
                case 56:
                    // Point
                    String lat = record.get(i); // 纬度
                    String lng = record.get(++i); //经度
                    if (StringUtils.isBlank(lat)) { // 模拟经纬度数据，如果经纬度为空，那么会报 Null geometry in feature
                        Double[] d = GDELTUtils.getLatAndLng(record);
                        lng = d[0].toString();
                        lat = d[1].toString();
                    }
                    builder.set(i - ++j, "POINT (" + lng + " " + lat + ")");
                    break;
                default:
                    builder.set(i - j, record.get(i));
            }
        }
        builder.featureUserData(Hints.USE_PROVIDED_FID, Boolean.TRUE);
        SimpleFeature feature = builder.buildFeature(globalEventId);
        builder.reset();
        return feature;
    }
}
