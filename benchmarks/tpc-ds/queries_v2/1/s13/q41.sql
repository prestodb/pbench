-- q41.sql

select  distinct(i_product_name)
 from item i1
 where i_manufact_id between 674 and 674+40 
   and (select count(*) as item_cnt
        from item
        where (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'orange' or i_color = 'mint') and 
        (i_units = 'Pallet' or i_units = 'Gram') and
        (i_size = 'small' or i_size = 'medium')
        ) or
        (i_category = 'Women' and
        (i_color = 'hot' or i_color = 'cream') and
        (i_units = 'Carton' or i_units = 'Each') and
        (i_size = 'extra large' or i_size = 'economy')
        ) or
        (i_category = 'Men' and
        (i_color = 'black' or i_color = 'drab') and
        (i_units = 'Dram' or i_units = 'Ton') and
        (i_size = 'N/A' or i_size = 'large')
        ) or
        (i_category = 'Men' and
        (i_color = 'thistle' or i_color = 'slate') and
        (i_units = 'Bundle' or i_units = 'Cup') and
        (i_size = 'small' or i_size = 'medium')
        ))) or
       (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'azure' or i_color = 'chocolate') and 
        (i_units = 'Box' or i_units = 'Unknown') and
        (i_size = 'small' or i_size = 'medium')
        ) or
        (i_category = 'Women' and
        (i_color = 'wheat' or i_color = 'burnished') and
        (i_units = 'Pound' or i_units = 'Tsp') and
        (i_size = 'extra large' or i_size = 'economy')
        ) or
        (i_category = 'Men' and
        (i_color = 'aquamarine' or i_color = 'almond') and
        (i_units = 'Oz' or i_units = 'N/A') and
        (i_size = 'N/A' or i_size = 'large')
        ) or
        (i_category = 'Men' and
        (i_color = 'lemon' or i_color = 'midnight') and
        (i_units = 'Ounce' or i_units = 'Gross') and
        (i_size = 'small' or i_size = 'medium')
        )))) > 0
 order by i_product_name
 limit 100;
