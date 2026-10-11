CREATE TABLE purchase_order (
    id           BIGSERIAL    PRIMARY KEY,
    product_code VARCHAR(50)  NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    quantity     INTEGER      NOT NULL CHECK (quantity > 0),
    status       VARCHAR(20)  NOT NULL,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT now()
);