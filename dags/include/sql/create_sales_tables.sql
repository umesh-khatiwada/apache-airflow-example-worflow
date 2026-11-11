-- Used by 20_capstone_sales_etl. Safe to run many times (IF NOT EXISTS).
CREATE TABLE IF NOT EXISTS sales_clean (
    order_id     INTEGER        NOT NULL,
    order_date   DATE           NOT NULL,
    region       VARCHAR(20)    NOT NULL,
    product      VARCHAR(50)    NOT NULL,
    quantity     INTEGER        NOT NULL CHECK (quantity > 0),
    unit_price   NUMERIC(10, 2) NOT NULL,
    revenue      NUMERIC(12, 2) NOT NULL,
    customer_id  VARCHAR(10),
    batch_id     VARCHAR(100)   NOT NULL,   -- which DAG run loaded the row (for idempotency)
    loaded_at    TIMESTAMP      NOT NULL DEFAULT NOW(),
    PRIMARY KEY (order_id, batch_id)
);

CREATE TABLE IF NOT EXISTS sales_daily_summary (
    batch_id     VARCHAR(100)   NOT NULL,
    region       VARCHAR(20)    NOT NULL,
    orders       INTEGER        NOT NULL,
    units        INTEGER        NOT NULL,
    revenue      NUMERIC(12, 2) NOT NULL,
    PRIMARY KEY (batch_id, region)
);
