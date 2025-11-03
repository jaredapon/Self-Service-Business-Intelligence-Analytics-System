CREATE TABLE holtwinters_results (
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

CREATE TABLE association_rules (
    bundle_id VARCHAR(16),
    antecedents_names VARCHAR(128),
    consequents_names VARCHAR(128),
    support FLOAT,
    confidence FLOAT,
    lift FLOAT,
    leverage FLOAT,
    conviction FLOAT,
    category VARCHAR(32)
);

CREATE TABLE ped_summary (
    bundle_id VARCHAR(16),
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
    r2_logspace FLOAT,
    plot_path VARCHAR(256)
);