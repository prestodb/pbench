-- q41.sql

select  distinct(i_product_name)
 from item i1
 where i_manufact_id between 901 and 901+40 
   and (select count(*) as item_cnt
        from item
        where (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'salmon' or i_color = 'dark') and 
        (i_units = 'Tsp' or i_units = 'Cup') and
        (i_size = 'small' or i_size = 'economy')
        ) or
        (i_category = 'Women' and
        (i_color = 'moccasin' or i_color = 'midnight') and
        (i_units = 'Box' or i_units = 'Pallet') and
        (i_size = 'petite' or i_size = 'medium')
        ) or
        (i_category = 'Men' and
        (i_color = 'seashell' or i_color = 'lawn') and
        (i_units = 'Bunch' or i_units = 'Dram') and
        (i_size = 'N/A' or i_size = 'large')
        ) or
        (i_category = 'Men' and
        (i_color = 'burnished' or i_color = 'yellow') and
        (i_units = 'Oz' or i_units = 'Ounce') and
        (i_size = 'small' or i_size = 'economy')
        ))) or
       (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'mint' or i_color = 'azure') and 
        (i_units = 'Tbl' or i_units = 'N/A') and
        (i_size = 'small' or i_size = 'economy')
        ) or
        (i_category = 'Women' and
        (i_color = 'puff' or i_color = 'khaki') and
        (i_units = 'Pound' or i_units = 'Gross') and
        (i_size = 'petite' or i_size = 'medium')
        ) or
        (i_category = 'Men' and
        (i_color = 'almond' or i_color = 'firebrick') and
        (i_units = 'Each' or i_units = 'Carton') and
        (i_size = 'N/A' or i_size = 'large')
        ) or
        (i_category = 'Men' and
        (i_color = 'pale' or i_color = 'cream') and
        (i_units = 'Gram' or i_units = 'Dozen') and
        (i_size = 'small' or i_size = 'economy')
        )))) > 0
 order by i_product_name
 limit 100;
