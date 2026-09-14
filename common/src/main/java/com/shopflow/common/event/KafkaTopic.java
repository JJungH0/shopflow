package com.shopflow.common.event;

public final class KafkaTopic {

    public static final String ORDER_CREATED = "order.created";
    public static final String STOCK_DECREASED = "stock.decreased";
    public static final String STOCK_FAILED = "stock.failed";

    private KafkaTopic() {}
}
