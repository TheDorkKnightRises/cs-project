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

public class VolumeTradedOfInstrumentWithEntityExtractorTest  extends AbstractSparkUnitTest{
    private Rfq rfq;

    @Before
    public void setup() {
        rfq = new Rfq();
        rfq.setEntityId(5561279226039690843L);
        rfq.setIsin("AT0000A0VRQ6");
    }

    @Test
    public void checkVolumeWhenInstrumentAndEntityMatch() {

        String filePath = getClass().getResource("volume-traded-1.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);

        VolumeTradedOfInstrumentWithEntityExtractor extractor = new VolumeTradedOfInstrumentWithEntityExtractor();
        extractor.setCurrent(new DateTime(2018, 6, 14, 0, 0));

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object result1 = meta.get(RfqMetadataFieldNames.volumeTradedOfInstrumentWithEntityPastWeek);
        Object result2 = meta.get(RfqMetadataFieldNames.volumeTradedOfInstrumentWithEntityPastMonth);
        Object result3 = meta.get(RfqMetadataFieldNames.volumeTradedOfInstrumentWithEntityPastYear);

        assertEquals(1_350_000L, result1);
        assertEquals(1_350_000L, result2);
        assertEquals(1_350_000L, result3);
    }

    @Test
    public void CheckVolumeWhenInstrumentAndEntityDontMatch() {

        String filePath = getClass().getResource("volume-traded-1.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);

        //all test trade data are for 2018 so this will cause no matches
        VolumeTradedOfInstrumentWithEntityExtractor extractor = new VolumeTradedOfInstrumentWithEntityExtractor();
        extractor.setCurrent(new DateTime(2019, 8, 15, 0, 0));

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object result1 = meta.get(RfqMetadataFieldNames.volumeTradedOfInstrumentWithEntityPastWeek);
        Object result2 = meta.get(RfqMetadataFieldNames.volumeTradedOfInstrumentWithEntityPastMonth);
        Object result3 = meta.get(RfqMetadataFieldNames.volumeTradedOfInstrumentWithEntityPastYear);

        assertEquals(0L, result1);
        assertEquals(0L, result2);
        assertEquals(0L, result3);

    }

}
