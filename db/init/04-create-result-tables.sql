-- Connect to the 'booklatte' database to ensure tables are created in the correct place.
\c booklatte;

-- Result Tables for Advanced Analytics
CREATE TABLE IF NOT EXISTS holtwinters_results_all (
    date DATE,
    bundle_units FLOAT,
    antecedent_units FLOAT,
    consequent_units FLOAT,
    bundle_units_forecast FLOAT,
    bundle_units_adjusted_forecast FLOAT,
    antecedent_units_forecast FLOAT,
    antecedent_units_after_cannibalization FLOAT,
    consequent_units_forecast FLOAT,
    consequent_units_after_cannibalization FLOAT,
    bundle_row INTEGER,
    bundle_id VARCHAR(16),
    category VARCHAR(32)
);

CREATE TABLE IF NOT EXISTS association_rules (
    bundle_id VARCHAR(16) PRIMARY KEY,
    antecedents_names VARCHAR(128),
    consequents_names VARCHAR(128),
    support FLOAT,
    confidence FLOAT,
    lift FLOAT,
    leverage FLOAT,
    conviction FLOAT,
    combined_score FLOAT,
    category VARCHAR(32)
);

CREATE TABLE IF NOT EXISTS ped_summary (
    bundle_id VARCHAR(16) PRIMARY KEY,
    category VARCHAR(32),
    rule_row INTEGER,
    product_id_1 VARCHAR(32),
    product_id_2 VARCHAR(32),
    product_name_1 VARCHAR(128),
    product_name_2 VARCHAR(128),
    mode VARCHAR(32),
    n_price_points INTEGER,
    elasticity_epsilon FLOAT,
    intercept_logk FLOAT,
    r2_logspace FLOAT
);

CREATE TABLE IF NOT EXISTS nlp_optimization_results (
    bundle_id VARCHAR(16) PRIMARY KEY,
    bundle_name VARCHAR(255),
    category VARCHAR(32),
    product_a VARCHAR(255),
    product_b VARCHAR(255),
    product_a_price DECIMAL(10, 2),
    product_b_price DECIMAL(10, 2),
    current_price_total DECIMAL(10, 2),
    product_a_cogs DECIMAL(10, 2),
    product_b_cogs DECIMAL(10, 2),
    cogs_total DECIMAL(10, 2),
    elasticity_epsilon DOUBLE PRECISION,
    base_demand_k DOUBLE PRECISION,
    r_squared DOUBLE PRECISION,
    n_points INTEGER,
    bundle_price_recommended DECIMAL(10, 2),
    quantity_demanded DOUBLE PRECISION,
    profit DECIMAL(12, 2),
    price_cap DECIMAL(10, 2),
    min_discount_pct DECIMAL(10, 2),
    optimization_success BOOLEAN
);