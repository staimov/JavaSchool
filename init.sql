CREATE TABLE IF NOT EXISTS transactions
(
    id VARCHAR(100) PRIMARY KEY,
    operation_type VARCHAR(100) NOT NULL,
    amount NUMERIC NOT NULL,
    account VARCHAR(100) NOT NULL,
    time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    offset_key BIGSERIAL NOT NULL
);
