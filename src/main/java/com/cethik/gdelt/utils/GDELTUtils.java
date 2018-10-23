package com.cethik.gdelt.utils;

import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * @author yinlei
 * @since 2018/10/23 9:55
 */
public class GDELTUtils {
    private static final Logger LOGGER = LogManager.getLogger(GDELTUtils.class);

    /**
     * 解压zip文件，返回流
     *
     * @param originFile zip文件。
     * @return 如果目标文件不是zip文件抛异常
     */
    public static InputStream decompress(File originFile) {
        InputStream is = null;
        try {
            ZipFile zipFile = new ZipFile(originFile);
            ZipEntry zipEntry;
            Enumeration<? extends ZipEntry> entry = zipFile.entries();
            while (entry.hasMoreElements()) { // 这个处理多个，其实我们的业务就一个
                zipEntry = entry.nextElement();
                is = zipFile.getInputStream(zipEntry);
            }
        } catch (Exception e) {
            LOGGER.error("解压zip文件错误。" + e.getMessage());
        }
        return is;
    }

    public static double[] getLatAndLng(CSVRecord record) {
        String lat = record.get(53);
        if (StringUtils.isBlank(lat)) {
            lat = record.get(46);
            if (StringUtils.isBlank(lat)) {
                lat = record.get(39);
            }
        }
        String lng = record.get(54);
        if (StringUtils.isBlank(lng)) {
            lng = record.get(47);
            if (StringUtils.isBlank(lng)) {
                lng = record.get(40);
            }
        }

        double latitude;
        double longitude;
        if (StringUtils.isAnyBlank(lat, lng)) {
            latitude = getLat();
            longitude = getLng();
            LOGGER.debug("没有获取到经纬度,模拟的经纬度是lat=[" + latitude + "], lng=[" + longitude + "]");
        } else {
            LOGGER.debug("经纬度是lat=[" + lat + "], lng=[" + lng + "]");
            latitude = Double.parseDouble(lat);
            longitude = Double.parseDouble(lng);
        }
        return new double[]{latitude, longitude};
    }

    public static double getLatOrLng(double min, double max) {
        BigDecimal db = new BigDecimal(Math.random() * (max - min) + min);
        return db.setScale(6, BigDecimal.ROUND_HALF_UP).doubleValue(); // 小数后6位
    }

    /**
     * 维度的范围 [-90,90]
     * @return 维度
     */
    public static double getLat() {
        return getLatOrLng(3, 53);
    }

    /**
     * 经度的范围 [-180,180]
     * @return 经度
     */
    public static double getLng() {
        return getLatOrLng(73, 135);
    }
}
