CREATE TABLE products (
    product_id text PRIMARY KEY,
    name text NOT NULL,
    price_cents bigint NOT NULL CHECK (price_cents >= 0),
    initial_stock bigint NOT NULL CHECK (initial_stock >= 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE orders (
    order_id uuid PRIMARY KEY,
    event_id uuid NOT NULL UNIQUE,
    user_id text NOT NULL,
    product_id text NOT NULL REFERENCES products(product_id),
    quantity integer NOT NULL CHECK (quantity BETWEEN 1 AND 10),
    idempotency_key text NOT NULL,
    status text NOT NULL CHECK (status IN ('PENDING', 'CONFIRMED', 'REJECTED')),
    failure_reason text,
    requested_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (user_id, idempotency_key)
);

CREATE INDEX orders_user_created_idx ON orders(user_id, created_at DESC);
CREATE INDEX orders_product_created_idx ON orders(product_id, created_at DESC);

CREATE TABLE processed_events (
    event_id uuid PRIMARY KEY,
    event_type text NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO products(product_id, name, price_cents, initial_stock) VALUES
    ('sku-phone', 'FlashFlow Phone', 399900, 100000),
    ('sku-headphones', 'FlashFlow Headphones', 49900, 250000),
    ('sku-keyboard', 'FlashFlow Mechanical Keyboard', 89900, 150000)
ON CONFLICT (product_id) DO NOTHING;
