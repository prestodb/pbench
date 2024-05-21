-- customer
SELECT 'custkey', checksum(custkey) FROM customer
UNION ALL
SELECT 'name', checksum(name) FROM customer
UNION ALL
SELECT 'address', checksum(address) FROM customer
UNION ALL
SELECT 'nationkey', checksum(nationkey) FROM customer
UNION ALL
SELECT 'phone', checksum(phone) FROM customer
UNION ALL
SELECT 'acctbal', checksum(acctbal) FROM customer
UNION ALL
SELECT 'mktsegment', checksum(mktsegment) FROM customer
UNION ALL
SELECT 'comment', checksum(comment) FROM customer
ORDER BY 1;

-- orders
SELECT 'orderkey', checksum(orderkey) FROM orders
UNION ALL
SELECT 'custkey', checksum(custkey) FROM orders
UNION ALL
SELECT 'orderstatus', checksum(orderstatus) FROM orders
UNION ALL
SELECT 'totalprice', checksum(totalprice) FROM orders
UNION ALL
SELECT 'orderdate', checksum(orderdate) FROM orders
UNION ALL
SELECT 'orderpriority', checksum(orderpriority) FROM orders
UNION ALL
SELECT 'clerk', checksum(clerk) FROM orders
UNION ALL
SELECT 'shippriority', checksum(shippriority) FROM orders
UNION ALL
SELECT 'comment', checksum(comment) FROM orders
ORDER BY 1;

-- lineitem
SELECT 'orderkey', checksum(orderkey) FROM lineitem
UNION ALL
SELECT 'partkey', checksum(partkey) FROM lineitem
UNION ALL
SELECT 'suppkey', checksum(suppkey) FROM lineitem
UNION ALL
SELECT 'linenumber', checksum(linenumber) FROM lineitem
UNION ALL
SELECT 'quantity', checksum(quantity) FROM lineitem
UNION ALL
SELECT 'extendedprice', checksum(extendedprice) FROM lineitem
UNION ALL
SELECT 'discount', checksum(discount) FROM lineitem
UNION ALL
SELECT 'tax', checksum(tax) FROM lineitem
UNION ALL
SELECT 'returnflag', checksum(returnflag) FROM lineitem
UNION ALL
SELECT 'linestatus', checksum(linestatus) FROM lineitem
UNION ALL
SELECT 'shipdate', checksum(shipdate) FROM lineitem
UNION ALL
SELECT 'commitdate', checksum(commitdate) FROM lineitem
UNION ALL
SELECT 'receiptdate', checksum(receiptdate) FROM lineitem
UNION ALL
SELECT 'shipinstruct', checksum(shipinstruct) FROM lineitem
UNION ALL
SELECT 'shipmode', checksum(shipmode) FROM lineitem
UNION ALL
SELECT 'comment', checksum(comment) FROM lineitem
ORDER BY 1;

-- region
SELECT 'regionkey', checksum(regionkey) FROM region
UNION ALL
SELECT 'name', checksum(name) FROM region
UNION ALL
SELECT 'comment', checksum(comment) FROM region
ORDER BY 1;

-- nation
SELECT 'nationkey', checksum(nationkey) FROM nation
UNION ALL
SELECT 'name', checksum(name) FROM nation
UNION ALL
SELECT 'regionkey', checksum(regionkey) FROM nation
UNION ALL
SELECT 'comment', checksum(comment) FROM nation
ORDER BY 1;

-- part
SELECT 'partkey', checksum(partkey) FROM part
UNION ALL
SELECT 'name', checksum(name) FROM part
UNION ALL
SELECT 'mfgr', checksum(mfgr) FROM part
UNION ALL
SELECT 'brand', checksum(brand) FROM part
UNION ALL
SELECT 'type', checksum(type) FROM part
UNION ALL
SELECT 'size', checksum(size) FROM part
UNION ALL
SELECT 'container', checksum(container) FROM part
UNION ALL
SELECT 'retailprice', checksum(retailprice) FROM part
UNION ALL
SELECT 'comment', checksum(comment) FROM part
ORDER BY 1;

-- partsupp
SELECT 'partkey', checksum(partkey) FROM partsupp
UNION ALL
SELECT 'suppkey', checksum(suppkey) FROM partsupp
UNION ALL
SELECT 'availqty', checksum(availqty) FROM partsupp
UNION ALL
SELECT 'supplycost', checksum(supplycost) FROM partsupp
UNION ALL
SELECT 'comment', checksum(comment) FROM partsupp
ORDER BY 1;

-- supplier
SELECT 'suppkey', checksum(suppkey) FROM supplier
UNION ALL
SELECT 'name', checksum(name) FROM supplier
UNION ALL
SELECT 'address', checksum(address) FROM supplier
UNION ALL
SELECT 'nationkey', checksum(nationkey) FROM supplier
UNION ALL
SELECT 'phone', checksum(phone) FROM supplier
UNION ALL
SELECT 'acctbal', checksum(acctbal) FROM supplier
UNION ALL
SELECT 'comment', checksum(comment) FROM supplier
ORDER BY 1;
