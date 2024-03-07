--#BGBLK 3

 --set current schema bdinsights; 
-- Large Aggregation
SELECT MAX(a), MAX(b), MAX(c), MAX(d), MAX(e), MAX(f), MAX(g), MAX(h), MAX(i), MAX(j), MAX(l), MAX(m), MAX(n),
  MAX(o), MAX(p), MAX(q), MAX(r), MAX(s), MAX(t), MAX(u), MAX(v), MAX(w), MAX(x), MAX(y), MAX(z),
  MAX(aa), MAX(ab), MAX(ac), MAX(ad), MAX(ae), MAX(af), MAX(ag), MAX(ah), MAX(ai), MAX(aj), MAX(al), MAX(am), MAX(an),
  MAX(ao), MAX(ap), MAX(aq), MAX(ar), MAX(as1), MAX(at), MAX(au), MAX(av), MAX(aw), MAX(ax), MAX(ay), MAX(az),
  MAX(ba), MAX(bb), MAX(bc), MAX(bd), MAX(be), MAX(bf), MAX(bg), MAX(bh), MAX(bi), MAX(bj), MAX(bl), MAX(bm), MAX(bn),
  MAX(bo), MAX(bp), MAX(bq), MAX(br), MAX(bs), MAX(bt), MAX(bu), MAX(bv), MAX(bw)
FROM (
SELECT 
  MIN(cs_wholesale_cost) a, MAX(cs_wholesale_cost) b, AVG(cs_wholesale_cost) c, SUM(cs_wholesale_cost) d, COUNT(cs_wholesale_cost) e,
  MIN(cs_list_price) f, MAX(cs_list_price) g, AVG(cs_list_price) h, SUM(cs_list_price) i, COUNT(cs_list_price) j,
  MIN(cs_sales_price) k, MAX(cs_sales_price) l, AVG(cs_sales_price) m, SUM(cs_sales_price) n, COUNT(cs_sales_price) o,
  MIN(cs_ext_discount_amt) p, MAX(cs_ext_discount_amt) q, AVG(cs_ext_discount_amt) r, SUM(cs_ext_discount_amt) s, COUNT(cs_ext_discount_amt) t,
  MIN(cs_ext_sales_price) u, MAX(cs_ext_sales_price) v, AVG(cs_ext_sales_price) w, SUM(cs_ext_sales_price) x, COUNT(cs_ext_sales_price) y,
  MIN(cs_ext_wholesale_cost) z, MAX(cs_ext_wholesale_cost) aa, AVG(cs_ext_wholesale_cost) ab, SUM(cs_ext_wholesale_cost) ac, COUNT(cs_ext_wholesale_cost) ad,
  MIN(cs_ext_list_price) ae, MAX(cs_ext_list_price) af, AVG(cs_ext_list_price) ag, SUM(cs_ext_list_price) ah, COUNT(cs_ext_list_price) ai,
  MIN(cs_ext_tax) aj, MAX(cs_ext_tax) ak, AVG(cs_ext_tax) al, SUM(cs_ext_tax) am, COUNT(cs_ext_tax) an,
  MIN(cs_coupon_amt) ao, MAX(cs_coupon_amt) ap, AVG(cs_coupon_amt) aq, SUM(cs_coupon_amt) ar, COUNT(cs_coupon_amt) as1,
  MIN(cs_ext_ship_cost) at, MAX(cs_ext_ship_cost) au, AVG(cs_ext_ship_cost) av, SUM(cs_ext_ship_cost) aw, COUNT(cs_ext_ship_cost) ax,
  MIN(cs_net_paid) ay, MAX(cs_net_paid) az, AVG(cs_net_paid) ba, SUM(cs_net_paid) bb, COUNT(cs_net_paid) bc,
  MIN(cs_net_paid_inc_tax) bd, MAX(cs_net_paid_inc_tax) be, AVG(cs_net_paid_inc_tax) bf, SUM(cs_net_paid_inc_tax) bg, COUNT(cs_net_paid_inc_tax) bh,
  MIN(cs_net_paid_inc_ship) bi, MAX(cs_net_paid_inc_ship) bj, AVG(cs_net_paid_inc_ship) bk, SUM(cs_net_paid_inc_ship) bl, COUNT(cs_net_paid_inc_ship) bm,
  MIN(cs_net_paid_inc_ship_tax) bn, MAX(cs_net_paid_inc_ship_tax) bo, AVG(cs_net_paid_inc_ship_tax) bp, SUM(cs_net_paid_inc_ship_tax) bq, COUNT(cs_net_paid_inc_ship_tax) br,
  MIN(cs_net_profit) bs, MAX(cs_net_profit) bt, AVG(cs_net_profit) bu, SUM(cs_net_profit) bv, COUNT(cs_net_profit) bw
FROM catalog_sales
GROUP BY cs_sold_date_sk, cs_quantity
) foo
;

--#EOBLK
