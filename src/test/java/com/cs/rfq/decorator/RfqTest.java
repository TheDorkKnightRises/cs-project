package com.cs.rfq.decorator;

import org.junit.Test;

import static org.junit.Assert.*;

public class RfqTest {

    @Test
    public void testJSonFactoryMethod() {
        String validRfqJson = "{" +
                "'id': '123ABC', " +
                "'traderId': 3351266293154445953, " +
                "'entityId': 5561279226039690843, " +
                "'instrumentId': 'AT0000383864', " +
                "'qty': 250000, " +
                "'price': 1.58, " +
                "'side': '1' " +
                "}";

        Rfq rfq = Rfq.fromJson(validRfqJson);

        assertEquals("123ABC", rfq.getId());
        assertEquals((Long) 3351266293154445953L, rfq.getTraderId());
        assertEquals((Long) 5561279226039690843L, rfq.getEntityId());
        assertEquals("AT0000383864", rfq.getIsin());
        assertEquals((Long) 250000L, rfq.getQuantity());
        assertEquals((Double) 1.58, rfq.getPrice());
        assertEquals("1", rfq.getSide());
        assertTrue(rfq.isBuySide());
        assertFalse(rfq.isSellSide());
    }
}
