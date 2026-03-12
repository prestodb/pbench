-- q41.sql

select  distinct(i_product_name)
 from item i1
 where i_manufact_id between 694 and 694+40 
   and (select count(*) as item_cnt
        from item
        where (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'chartreuse' or i_color = 'magenta') and 
        (i_units = 'Oz' or i_units = 'Each') and
        (i_size = 'large' or i_size = 'extra large')
        ) or
        (i_category = 'Women' and
        (i_color = 'hot' or i_color = 'cream') and
        (i_units = 'Bunch' or i_units = 'Dram') and
        (i_size = 'small' or i_size = 'economy')
        ) or
        (i_category = 'Men' and
        (i_color = 'azure' or i_color = 'red') and
        (i_units = 'Gross' or i_units = 'Ounce') and
        (i_size = 'petite' or i_size = 'medium')
        ) or
        (i_category = 'Men' and
        (i_color = 'cyan' or i_color = 'honeydew') and
        (i_units = 'Case' or i_units = 'Tbl') and
        (i_size = 'large' or i_size = 'extra large')
        ))) or
       (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'pink' or i_color = 'puff') and 
        (i_units = 'Dozen' or i_units = 'Unknown') and
        (i_size = 'large' or i_size = 'extra large')
        ) or
        (i_category = 'Women' and
        (i_color = 'ghost' or i_color = 'cornflower') and
        (i_units = 'Cup' or i_units = 'Pound') and
        (i_size = 'small' or i_size = 'economy')
        ) or
        (i_category = 'Men' and
        (i_color = 'almond' or i_color = 'navy') and
        (i_units = 'Bundle' or i_units = 'Tsp') and
        (i_size = 'petite' or i_size = 'medium')
        ) or
        (i_category = 'Men' and
        (i_color = 'plum' or i_color = 'lace') and
        (i_units = 'Pallet' or i_units = 'Box') and
        (i_size = 'large' or i_size = 'extra large')
        )))) > 0
 order by i_product_name
 limit 100;
