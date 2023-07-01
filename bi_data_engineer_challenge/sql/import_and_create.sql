CREATE SCHEMA "customer_data";

CREATE SCHEMA "order_data";

CREATE SCHEMA "loads";

CREATE EXTENSION IF NOT EXISTS dblink;

SELECT dblink_connect('conn','dbname=raw_data');

CREATE TABLE tmp_import AS
SELECT 
    * 
FROM
    dblink('conn', 'SELECT * FROM imports.data') T(order_id bigint,
 first_name text,
 last_name text,
 date_of_birth date,
 phone text,
 email text,
 street text,
 city text,
 postal_code bigint,
 state text,
 order_date date,
 payment_type text,
 product_name text,
 price double precision,
 quantity bigint,
 order_total double precision);
 
ALTER TABLE tmp_import SET SCHEMA "loads";

ALTER TABLE loads.tmp_import ADD COLUMN customer_id varchar(255);

ALTER TABLE loads.tmp_import ADD COLUMN address_id varchar(255);

UPDATE loads.tmp_import
SET customer_id = SHA256(CONCAT(first_name,'-',last_name,'-',date_of_birth)::bytea);

UPDATE loads.tmp_import
SET address_id = SHA256(CONCAT(street,'-',city,'-',postal_code)::bytea);

CREATE TABLE customer_data.customers AS 
SELECT DISTINCT
    customer_id
    ,first_name
    ,last_name
    ,date_of_birth
    ,phone
    ,email
FROM loads.tmp_import;

ALTER TABLE customer_data.customers ADD PRIMARY KEY (customer_id);

CREATE TABLE customer_data.address_history AS 
SELECT DISTINCT
    address_id
    ,customer_id
    ,street as street_number
    ,city
    ,postal_code
    ,state
    ,MIN(order_date) OVER (PARTITION BY customer_id,address_id) AS start_date
    ,MAX(order_date) OVER (PARTITION BY customer_id,address_id) AS end_date
FROM loads.tmp_import;

ALTER TABLE customer_data.address_history ADD PRIMARY KEY (address_id);

CREATE TABLE order_data.order_details AS 
SELECT DISTINCT
   product_name
   ,order_id
   ,price
   ,quantity
   --,CONCAT("N/A") as comments
FROM loads.tmp_import;

ALTER TABLE order_data.order_details ADD status varchar(255);

UPDATE order_data.order_details
SET status = 'None';

ALTER TABLE order_data.order_details ADD comments varchar(255);

UPDATE order_data.order_details
SET comments = 'None';

CREATE TABLE order_data.orders AS 
SELECT DISTINCT
   order_id
   ,customer_id
   ,order_date
   ,payment_type AS payment_method_id
   --,CONCAT("N/A") as comments
FROM loads.tmp_import;

ALTER TABLE order_data.orders ADD COLUMN comments varchar(255);

UPDATE order_data.orders
SET comments = 'None';

ALTER TABLE order_data.orders ADD PRIMARY KEY (order_id);

