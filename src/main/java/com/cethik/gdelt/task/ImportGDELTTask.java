package com.cethik.gdelt.task;

import com.cethik.geomesa.datastore.GeoMesaDataSource;

import java.util.TimerTask;

/**
 * 导入 GDELT 2.0 数据的定时任务
 *
 * @author yinlei
 * @since 2018/10/22 11:11
 */
public class ImportGDELTTask extends TimerTask {

    private GeoMesaDataSource geoMesaDataSource;

    @Override
    public void run() {
        System.out.println("gagagaaggagagaga");
    }

    public void setGeoMesaDataSource(GeoMesaDataSource geoMesaDataSource) {
        this.geoMesaDataSource = geoMesaDataSource;
    }
}
