
-- 0️ CLEAN START (DROP TABLES IF EXIST)

DROP TABLE IF EXISTS dispatch_orders CASCADE;
DROP TABLE IF EXISTS production_orders CASCADE;
DROP TABLE IF EXISTS mrp_results CASCADE;
DROP TABLE IF EXISTS inventory_stock CASCADE;
DROP TABLE IF EXISTS bom_master CASCADE;
DROP TABLE IF EXISTS products CASCADE;


-- 1️ MASTER TABLES
 
CREATE TABLE products (
    product_code VARCHAR(20) PRIMARY KEY,
    product_name TEXT,
    product_type VARCHAR(10) -- FG / SFG / RM
);

CREATE TABLE bom_master (
    parent_part VARCHAR(20),
    child_part  VARCHAR(20),
    qty_per     NUMERIC(10,2),
    PRIMARY KEY (parent_part, child_part)
);

CREATE TABLE inventory_stock (
    part_code VARCHAR(20) PRIMARY KEY,
    available_qty NUMERIC(10,2)
);

CREATE TABLE production_orders (
    order_id SERIAL PRIMARY KEY,
    fg_code VARCHAR(20),
    order_qty NUMERIC(10,2),
    mrp_status VARCHAR(10) DEFAULT 'NO'
);

CREATE TABLE mrp_results (
    fg_code VARCHAR(20),
    part_code VARCHAR(20),
    required_qty NUMERIC(10,2),
    available_qty NUMERIC(10,2),
    shortage_qty NUMERIC(10,2)
);

CREATE TABLE dispatch_orders (
    dispatch_id SERIAL PRIMARY KEY,
    fg_code VARCHAR(20),
    dispatch_qty NUMERIC(10,2),
    dispatch_date DATE DEFAULT CURRENT_DATE
);


-- 2️ PRODUCTS (FG + SFG + RM)

INSERT INTO products VALUES
-- Finished Goods
('FG001','Gear Box','FG'),
('FG002','Hydraulic Pump','FG'),
('FG003','Engine Assembly','FG'),

-- Sub Assemblies
('SFG001','Gear Unit','SFG'),
('SFG002','Shaft Assembly','SFG'),
('SFG003','Pump Housing','SFG'),
('SFG004','Valve Block','SFG'),
('SFG005','Cylinder Unit','SFG'),
('SFG006','Crank Unit','SFG'),

-- Raw Materials
('RM001','Steel Rod','RM'),
('RM002','Iron Casting','RM'),
('RM003','Bearing','RM'),
('RM004','Seal Kit','RM'),
('RM005','Bolt & Nut','RM'),
('RM006','Aluminium Block','RM'),
('RM007','Rubber Hose','RM'),
('RM008','Oil Filter','RM'),
('RM009','Piston','RM'),
('RM010','Gasket','RM');


-- 3️⃣ BOM (Heavy Multi-level)

INSERT INTO bom_master VALUES
-- FG001 BOM
('FG001','SFG001',1),
('FG001','SFG002',2),

-- FG002 BOM
('FG002','SFG003',1),
('FG002','SFG004',1),

-- FG003 BOM
('FG003','SFG005',2),
('FG003','SFG006',1),

-- SFG001 BOM
('SFG001','RM001',4),
('SFG001','RM003',2),

-- SFG002 BOM
('SFG002','RM002',2),
('SFG002','RM005',6),

-- SFG003 BOM
('SFG003','RM006',1),
('SFG003','RM004',2),

-- SFG004 BOM
('SFG004','RM005',4),
('SFG004','RM007',2),

-- SFG005 BOM
('SFG005','RM009',2),
('SFG005','RM010',3),

-- SFG006 BOM
('SFG006','RM001',3),
('SFG006','RM008',1);


-- 4️ INVENTORY (Realistic / Heavy)

INSERT INTO inventory_stock VALUES
('SFG001',10),
('SFG002',4),
('SFG003',6),
('SFG004',3),
('SFG005',2),
('SFG006',1),

('RM001',20),
('RM002',5),
('RM003',50),
('RM004',3),
('RM005',20),
('RM006',1),
('RM007',4),
('RM008',0),
('RM009',1),
('RM010',5);


-- 5️ PRODUCTION ORDERS

INSERT INTO production_orders (fg_code, order_qty) VALUES
('FG001',8),
('FG002',5),
('FG003',4);


-- 6️ MRP FUNCTION (Recursive CTE)

CREATE OR REPLACE FUNCTION run_mrp(p_fg VARCHAR, p_qty NUMERIC)
RETURNS VOID AS $$
BEGIN
    DELETE FROM mrp_results WHERE fg_code = p_fg;

    INSERT INTO mrp_results
    SELECT 
        p_fg,
        bt.child_part,
        SUM(bt.qty_per * p_qty) AS required_qty,
        COALESCE(i.available_qty,0) AS available_qty,
        SUM(bt.qty_per * p_qty) - COALESCE(i.available_qty,0) AS shortage_qty
    FROM (
        WITH RECURSIVE bom_tree AS (
            SELECT parent_part, child_part, qty_per
            FROM bom_master
            WHERE parent_part = p_fg
            UNION ALL
            SELECT b.parent_part, b.child_part, b.qty_per
            FROM bom_master b
            JOIN bom_tree bt ON bt.child_part = b.parent_part
        )
        SELECT * FROM bom_tree
    ) bt
    LEFT JOIN inventory_stock i ON bt.child_part = i.part_code
    GROUP BY bt.child_part, i.available_qty;

    UPDATE production_orders
    SET mrp_status = 'YES'
    WHERE fg_code = p_fg;
END;
$$ LANGUAGE plpgsql;


-- 7️ DISPATCH TRIGGER

CREATE OR REPLACE FUNCTION check_mrp_before_dispatch()
RETURNS TRIGGER AS $$
DECLARE
    v_status VARCHAR;
BEGIN
    SELECT mrp_status INTO v_status
    FROM production_orders
    WHERE fg_code = NEW.fg_code
    ORDER BY order_id DESC
    LIMIT 1;

    IF v_status IS NULL OR v_status = 'NO' THEN
        RAISE EXCEPTION 'MRP not run. Dispatch blocked!';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_check_mrp
BEFORE INSERT ON dispatch_orders
FOR EACH ROW
EXECUTE FUNCTION check_mrp_before_dispatch();


-- 8️ TEST MRP RUN

SELECT run_mrp('FG001',8);
SELECT run_mrp('FG002',5);
SELECT run_mrp('FG003',4);

SELECT * FROM mrp_results ORDER BY fg_code, shortage_qty DESC;


-- 9️ TREE VIEW (FG003 Example)

WITH RECURSIVE bom_tree AS (
    SELECT parent_part, child_part, qty_per, 1 AS level
    FROM bom_master
    WHERE parent_part = 'FG003'

    UNION ALL

    SELECT b.parent_part, b.child_part, b.qty_per, bt.level + 1
    FROM bom_master b
    JOIN bom_tree bt ON bt.child_part = b.parent_part
)
SELECT LPAD(' ', level * 4, ' ') || parent_part AS parent,
       child_part,
       qty_per,
       level
FROM bom_tree
ORDER BY level;


-- 10 DISPATCH TEST

-- Without MRP
-- INSERT INTO dispatch_orders (fg_code, dispatch_qty) VALUES ('FG002',1);

-- After MRP
INSERT INTO dispatch_orders (fg_code, dispatch_qty) VALUES ('FG003',1);
SELECT * FROM dispatch_orders;
