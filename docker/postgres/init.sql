-- Core-banking customers table captured by Debezium
CREATE TABLE IF NOT EXISTS public.customers (
    customer_id  INTEGER PRIMARY KEY,
    full_name    TEXT        NOT NULL,
    email        TEXT        NOT NULL,
    city         TEXT        NOT NULL,
    branch_code  TEXT        NOT NULL,
    risk_tier    TEXT        NOT NULL CHECK (risk_tier IN ('LOW', 'MEDIUM', 'HIGH')),
    kyc_status   TEXT        NOT NULL CHECK (kyc_status IN ('PENDING', 'VERIFIED', 'EXPIRED')),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- FULL replica identity: update/delete events carry the complete "before" image,
-- which the SCD2 job uses to close the previous version.
ALTER TABLE public.customers REPLICA IDENTITY FULL;

INSERT INTO public.customers (customer_id, full_name, email, city, branch_code, risk_tier, kyc_status) VALUES
    (1, 'Customer 1', 'cust1@example.com', 'Hyderabad', 'HYD001', 'LOW',    'VERIFIED'),
    (2, 'Customer 2', 'cust2@example.com', 'Bengaluru', 'BLR004', 'MEDIUM', 'PENDING'),
    (3, 'Customer 3', 'cust3@example.com', 'Chennai',   'MAA002', 'HIGH',   'VERIFIED'),
    (4, 'Customer 4', 'cust4@example.com', 'Mumbai',    'BOM010', 'LOW',    'EXPIRED'),
    (5, 'Customer 5', 'cust5@example.com', 'Pune',      'PNQ003', 'MEDIUM', 'VERIFIED')
ON CONFLICT (customer_id) DO NOTHING;
