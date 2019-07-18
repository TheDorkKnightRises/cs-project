package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.TradeDataLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class BuySellRatioExtractorTest extends AbstractSparkUnitTest{
    private Rfq rfq;

    @Before
    public void setup() {
        rfq = new Rfq();
        rfq.setEntityId(5561279226039690843L);
        rfq.setIsin("AT0000A0VRQ6");
    }

    @Test
    public void checkRatioWhenBothBuyAndSell() {

        String filePath = getClass().getResource("volume-traded-1.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);

        BuySellRatioExtractor extractor = new BuySellRatioExtractor();
        extractor.setCurrent(new DateTime(2018, 6, 14, 0, 0));

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object result1 = meta.get(RfqMetadataFieldNames.buySellRatioPastWeek);
        Object result2 = meta.get(RfqMetadataFieldNames.buySellRatioPastMonth);

        assertEquals("1.7", result1);
        assertEquals("1.7", result2);
    }

    @Test
    public void checkRatioWhenBuyButNoSell() {

        String filePath = getClass().getResource("volume-traded-2.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);

        //all test trade data are for 2018 so this will cause no matches
        BuySellRatioExtractor extractor = new BuySellRatioExtractor();
        extractor.setCurrent(new DateTime(2019, 7, 13, 0, 0));

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object result1 = meta.get(RfqMetadataFieldNames.buySellRatioPastWeek);
        Object result2 = meta.get(RfqMetadataFieldNames.buySellRatioPastMonth);

        assertEquals("Infinity", result1);
        assertEquals("Infinity", result2);

    }


    @Test
    public void checkRatioWhenNoTradesForInstrument() {

        String filePath = getClass().getResource("volume-traded-1.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);

        //all test trade data are for 2018 so this will cause no matches
        BuySellRatioExtractor extractor = new BuySellRatioExtractor();
        extractor.setCurrent(new DateTime(2019, 8, 15, 0, 0));

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object result1 = meta.get(RfqMetadataFieldNames.buySellRatioPastWeek);
        Object result2 = meta.get(RfqMetadataFieldNames.buySellRatioPastMonth);

        assertEquals("-1", result1);
        assertEquals("-1", result2);

    }

}
