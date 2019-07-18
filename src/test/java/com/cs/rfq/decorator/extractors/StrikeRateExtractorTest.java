package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.TradeDataLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class StrikeRateExtractorTest extends AbstractSparkUnitTest {
    private Rfq rfq;

    @Before
    public void setup() {
        rfq = new Rfq();
        rfq.setIsin("AT0000A0VRQ6");
        rfq.setTraderId(7704615737577737110L);
    }

    @Test
    public void checkStrikeRate() {

        String filePath = getClass().getResource("volume-traded-1.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);

        StrikeRateExtractor extractor = new StrikeRateExtractor();
        //extractor.setSince("2018-06-10");

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object result = meta.get(RfqMetadataFieldNames.strikeRate);

        assertEquals((float) 50, result);
//        assertEquals(138.4396, result);
    }
    
}
