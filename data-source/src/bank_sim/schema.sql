CREATE SCHEMA IF NOT EXISTS {schema};

CREATE TABLE IF NOT EXISTS {schema}.customers (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    region TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS {schema}.merchants (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    mcc TEXT NOT NULL CHECK (mcc ~ '^[0-9]{{4}}$'),
    region TEXT NOT NULL,
    channel TEXT NOT NULL CHECK (channel IN ('QR', 'POS')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS {schema}.accounts (
    id UUID PRIMARY KEY,
    customer_id UUID UNIQUE REFERENCES {schema}.customers(id),
    merchant_id UUID UNIQUE REFERENCES {schema}.merchants(id),
    currency TEXT NOT NULL DEFAULT 'VND' CHECK (currency = 'VND'),
    opening_balance BIGINT NOT NULL CHECK (opening_balance >= 0),
    balance BIGINT NOT NULL CHECK (balance >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK ((customer_id IS NOT NULL)::int + (merchant_id IS NOT NULL)::int = 1)
);

CREATE TABLE IF NOT EXISTS {schema}.transactions (
    id UUID PRIMARY KEY,
    request_id UUID NOT NULL UNIQUE,
    kind TEXT NOT NULL CHECK (kind IN ('PAYMENT', 'REFUND')),
    source_account_id UUID NOT NULL REFERENCES {schema}.accounts(id),
    destination_account_id UUID NOT NULL REFERENCES {schema}.accounts(id),
    amount BIGINT NOT NULL CHECK (amount > 0),
    currency TEXT NOT NULL DEFAULT 'VND' CHECK (currency = 'VND'),
    channel TEXT NOT NULL CHECK (channel IN ('QR', 'POS')),
    status TEXT NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'SUCCEEDED', 'FAILED')),
    error_code TEXT,
    original_transaction_id UUID REFERENCES {schema}.transactions(id),
    requested_decline BOOLEAN NOT NULL DEFAULT FALSE,
    occurred_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (source_account_id <> destination_account_id),
    CHECK ((kind = 'PAYMENT' AND original_transaction_id IS NULL)
        OR (kind = 'REFUND' AND original_transaction_id IS NOT NULL AND original_transaction_id <> id)),
    CHECK ((status = 'FAILED' AND error_code IS NOT NULL)
        OR (status IN ('PENDING', 'SUCCEEDED') AND error_code IS NULL))
);

CREATE UNIQUE INDEX IF NOT EXISTS one_successful_refund
    ON {schema}.transactions(original_transaction_id)
    WHERE kind = 'REFUND' AND status = 'SUCCEEDED';
CREATE INDEX IF NOT EXISTS transactions_occurred_at ON {schema}.transactions(occurred_at);
CREATE INDEX IF NOT EXISTS transactions_refund_candidates ON {schema}.transactions(occurred_at, id)
    WHERE kind = 'PAYMENT' AND status = 'SUCCEEDED';
CREATE INDEX IF NOT EXISTS transactions_updated_at ON {schema}.transactions(updated_at, id);
CREATE INDEX IF NOT EXISTS transactions_pending ON {schema}.transactions(created_at, id) WHERE status = 'PENDING';
CREATE INDEX IF NOT EXISTS transactions_original ON {schema}.transactions(original_transaction_id) WHERE original_transaction_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS transactions_source ON {schema}.transactions(source_account_id) WHERE status = 'SUCCEEDED';
CREATE INDEX IF NOT EXISTS transactions_destination ON {schema}.transactions(destination_account_id) WHERE status = 'SUCCEEDED';
