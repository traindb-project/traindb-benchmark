CREATE TABLE IF NOT EXISTS aisles (
  aisle_id int,
  aisle varchar(64),
  PRIMARY KEY(aisle_id)
);

CREATE TABLE IF NOT EXISTS departments (
  department_id int,
  department varchar(64),
  PRIMARY KEY(department_id)
);

CREATE TABLE IF NOT EXISTS order_products (
  order_id int,
  product_id int,
  add_to_cart_order int,
  reordered int,
  PRIMARY KEY(order_id, product_id)
);

CREATE TABLE IF NOT EXISTS orders (
  order_id int,
  user_id int,
  eval_set varchar(16),
  order_number int,
  order_dow int,
  order_hour_of_day int,
  days_since_prior_order float,
  PRIMARY KEY(order_id)
);

CREATE TABLE IF NOT EXISTS products (
    product_id int,
    product_name varchar(512),
    aisle_id int,
    department_id int,
  PRIMARY KEY(product_id)
);
