CREATE TABLE product (
    id    BIGSERIAL    PRIMARY KEY,
    code  VARCHAR(50)  NOT NULL UNIQUE,
    name  VARCHAR(255) NOT NULL,
    stock INTEGER      NOT NULL CHECK (stock >= 0)
);