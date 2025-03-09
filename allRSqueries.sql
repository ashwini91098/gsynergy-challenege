-- Step 1: Create schema in redshift

CREATE SCHEMA IF NOT EXISTS staging;
CREATE TABLE IF NOT EXISTS staging.fact_transactions (
    order_id BIGINT,
    line_id INT,
    type VARCHAR(50),
    dt TIMESTAMP,
    pos_site_id INT,
    sku_id INT,
    fscldt_id INT,
    price_substate_id INT,
    sales_units DECIMAL(10,2),
    sales_dollars DECIMAL(10,2),
    discount_dollars DECIMAL(10,2),
    original_order_id BIGINT,
    original_line_id INT,
    PRIMARY KEY (order_id, line_id)
);
CREATE TABLE IF NOT EXISTS staging.fact_averagecosts (
    fscldt_id INT,
    sku_id INT,
    average_unit_standardcost DECIMAL(10,4),
    average_unit_landedcost DECIMAL(10,4),
    PRIMARY KEY (fscldt_id, sku_id)
);
CREATE TABLE IF NOT EXISTS staging.hier_clnd (
    fscldt_id INT PRIMARY KEY,
    fscldt_label VARCHAR(50),
    fsclwk_id INT,
    fsclwk_label VARCHAR(50),
    fsclmth_id INT,
    fsclmth_label VARCHAR(50),
    fsclqrtr_id INT,
    fsclqrtr_label VARCHAR(50),
    fsclyr_id INT,
    fsclyr_label VARCHAR(50),
    ssn_id INT,
    ssn_label VARCHAR(50),
    ly_fscldt_id INT,
    lly_fscldt_id INT,
    fscldow INT,
    fscldom INT,
    fscldoq INT,
    fscldoy INT,
    fsclwoy INT,
    fsclmoy INT,
    fsclqoy INT,
    date DATE
);
CREATE TABLE IF NOT EXISTS staging.hier_prod (
    sku_id INT PRIMARY KEY,
    sku_label VARCHAR(255),
    stylclr_id INT,
    stylclr_label VARCHAR(255),
    styl_id INT,
    styl_label VARCHAR(255),
    subcat_id INT,
    subcat_label VARCHAR(255),
    cat_id INT,
    cat_label VARCHAR(255),
    dept_id INT,
    dept_label VARCHAR(255),
    issvc BOOLEAN,
    isasmbly BOOLEAN,
    isnfs BOOLEAN
);
CREATE TABLE IF NOT EXISTS staging.hier_invstatus (
    code_id VARCHAR(20) PRIMARY KEY,        
    code_label VARCHAR(100),   
    bckt_id VARCHAR(20),        
    bckt_label VARCHAR(100),    
    ownrshp_id VARCHAR(20),     
    ownrshp_label VARCHAR(100)
);
CREATE TABLE IF NOT EXISTS staging.hier_rtlloc (
    str INT PRIMARY KEY,
    str_label VARCHAR(255),
    dstr INT,
    dstr_label VARCHAR(255),
    rgn INT,
    rgn_label VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS staging.hier_possite (
    site_id INT PRIMARY KEY,
    site_label VARCHAR(255),
    subchnl_id INT,
    subchnl_label VARCHAR(255),
    chnl_id INT,
    chnl_label VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS staging.hier_invloc (
    loc INT PRIMARY KEY,
    loc_label VARCHAR(255),
    loctype INT,
    loctype_label VARCHAR(255)
);
CREATE TABLE staging.hier_hldy (
    hldy_id VARCHAR(50) PRIMARY KEY,
    hldy_label VARCHAR(100)
);
CREATE TABLE IF NOT EXISTS staging.hier_pricestate (
    substate_id VARCHAR(10) PRIMARY KEY,      
    substate_label VARCHAR(50),  
    state_id VARCHAR(10),         
    state_label VARCHAR(50) 
);


-- Step 2: Load data into tables from S3

COPY staging.fact_transactions
FROM 's3://mygsynergy/transformed/fact_transactions/'
IAM_ROLE 'arn:aws:iam::825765404402:role/RedshiftS3Role'
FORMAT AS CSV
DELIMITER ','
IGNOREHEADER 1
GZIP;

COPY staging.fact_averagecosts
FROM 's3://mygsynergy/transformed/fact_averagecosts/'
IAM_ROLE 'arn:aws:iam::825765404402:role/RedshiftS3Role'
FORMAT AS CSV
DELIMITER ','
IGNOREHEADER 1
GZIP;

COPY staging.hier_clnd
FROM 's3://mygsynergy/transformed/hier_clnd/'
IAM_ROLE 'arn:aws:iam::825765404402:role/RedshiftS3Role'
FORMAT AS CSV
DELIMITER ','
IGNOREHEADER 1
GZIP;

COPY staging.hier_prod
FROM 's3://mygsynergy/transformed/hier_prod/'
IAM_ROLE 'arn:aws:iam::825765404402:role/RedshiftS3Role'
FORMAT AS CSV
DELIMITER ','
IGNOREHEADER 1
GZIP;

COPY staging.hier_invstatus
FROM 's3://mygsynergy/transformed/hier_invstatus/'
IAM_ROLE 'arn:aws:iam::825765404402:role/RedshiftS3Role'
FORMAT AS CSV
DELIMITER ','
IGNOREHEADER 1
GZIP;

COPY staging.hier_rtlloc
FROM 's3://mygsynergy/transformed/hier_rtlloc/'
IAM_ROLE 'arn:aws:iam::825765404402:role/RedshiftS3Role'
FORMAT AS CSV
DELIMITER ','
IGNOREHEADER 1
GZIP;

COPY staging.hier_possite
FROM 's3://mygsynergy/transformed/hier_possite/'
IAM_ROLE 'arn:aws:iam::825765404402:role/RedshiftS3Role'
FORMAT AS CSV
DELIMITER ','
IGNOREHEADER 1
GZIP;

COPY staging.hier_invloc
FROM 's3://mygsynergy/transformed/hier_invloc/'
IAM_ROLE 'arn:aws:iam::825765404402:role/RedshiftS3Role'
FORMAT AS CSV
DELIMITER ','
IGNOREHEADER 1
GZIP;

COPY staging.hier_hldy
FROM 's3://mygsynergy/transformed/hier_hldy/'
IAM_ROLE 'arn:aws:iam::825765404402:role/RedshiftS3Role'
FORMAT AS CSV
DELIMITER ','
IGNOREHEADER 1
GZIP;

COPY staging.hier_pricestate
FROM 's3://mygsynergy/transformed/hier_pricestate/'
IAM_ROLE 'arn:aws:iam::825765404402:role/RedshiftS3Role'
FORMAT AS CSV
DELIMITER ','
IGNOREHEADER 1
GZIP;

-- Step 3: Check for load errors
SELECT * FROM stl_load_errors ORDER BY starttime DESC LIMIT 5;



--task 2.c in attached challenge
CREATE TABLE IF NOT EXISTS mview_weekly_sales (
    pos_site_id INT NOT NULL,
    sku_id INT NOT NULL,
    fsclwk_id INT NOT NULL,
    price_substate_id INT NOT NULL,
    type VARCHAR(50) NOT NULL,
    total_sales_units DECIMAL(18,2),
    total_sales_dollars DECIMAL(18,2),
    total_discount_dollars DECIMAL(18,2),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

--initial insert
INSERT INTO mview_weekly_sales (
    pos_site_id, sku_id, fsclwk_id, price_substate_id, type, 
    total_sales_units, total_sales_dollars, total_discount_dollars
)
SELECT 
    ft.pos_site_id, 
    ft.sku_id, 
    hc.fsclwk_id, 
    ft.price_substate_id, 
    ft.type, 
    SUM(ft.sales_units) AS total_sales_units,
    SUM(ft.sales_dollars) AS total_sales_dollars,
    SUM(ft.discount_dollars) AS total_discount_dollars
FROM staging.fact_transactions ft
JOIN staging.hier_clnd hc 
    ON ft.fscldt_id = hc.fscldt_id
WHERE 
    ft.pos_site_id IS NOT NULL 
    AND ft.sku_id IS NOT NULL 
    AND hc.fsclwk_id IS NOT NULL 
    AND ft.price_substate_id IS NOT NULL 
    AND ft.type IS NOT NULL
GROUP BY 
    ft.pos_site_id, ft.sku_id, hc.fsclwk_id, ft.price_substate_id, ft.type;


--task 2.d in challenge pdf - can be scheduled in redshift shown in video recording
--
BEGIN;

---DELETE only the affected rows (to avoid duplicates)
DELETE FROM mview_weekly_sales 
WHERE fsclwk_id IN (
    SELECT DISTINCT hc.fsclwk_id
    FROM staging.fact_transactions ft
    JOIN staging.hier_clnd hc 
        ON ft.fscldt_id = hc.fscldt_id
);

--INSERT the new aggregated data
INSERT INTO mview_weekly_sales (
    pos_site_id, sku_id, fsclwk_id, price_substate_id, type, 
    total_sales_units, total_sales_dollars, total_discount_dollars
)
SELECT 
    ft.pos_site_id, 
    ft.sku_id, 
    hc.fsclwk_id, 
    ft.price_substate_id, 
    ft.type, 
    SUM(ft.sales_units) AS total_sales_units,
    SUM(ft.sales_dollars) AS total_sales_dollars,
    SUM(ft.discount_dollars) AS total_discount_dollars
FROM staging.fact_transactions ft
JOIN staging.hier_clnd hc 
    ON ft.fscldt_id = hc.fscldt_id
WHERE 
    ft.pos_site_id IS NOT NULL 
    AND ft.sku_id IS NOT NULL 
    AND hc.fsclwk_id IS NOT NULL 
    AND ft.price_substate_id IS NOT NULL 
    AND ft.type IS NOT NULL
GROUP BY 
    ft.pos_site_id, ft.sku_id, hc.fsclwk_id, ft.price_substate_id, ft.type;

COMMIT;
