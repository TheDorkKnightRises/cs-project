package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.TradeDataLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class VolumeTradedOfInstrumentWithEntityExtractorTest  extends AbstractSparkUnitTest{
    private Rfq rfq;

    @Before
    public void setup() {
        rfq = new Rfq();
        rfq.setIsin("AT0000A0VRQ6");
    }

    @Test
    public void checkVolumeWhenInstrumentMatch() {

        String filePath = getClass().getResource("volume-traded-1.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);

        AveragePriceOfInstrumentExtractor extractor = new AveragePriceOfInstrumentExtractor();
        extractor.setSince("2018-06-10");

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object result = meta.get(RfqMetadataFieldNames.averageInstrumentPrice);

        assertEquals(125.977, result);
//        assertEquals(138.4396, result);
    }

    @Test
    public void CheckVolumeWhenNoInstrumentMatch() {

        String filePath = getClass().getResource("volume-traded-1.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);

        //all test trade data are for 2018 so this will cause no matches
        AveragePriceOfInstrumentExtractor extractor = new AveragePriceOfInstrumentExtractor();
        extractor.setSince("2019-06-17");

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object result = meta.get(RfqMetadataFieldNames.averageInstrumentPrice);

        assertEquals(0, result);

    }

}
