-- q41.sql

select  distinct(i_product_name)
 from item i1
 where i_manufact_id between 874 and 874+40 
   and (select count(*) as item_cnt
        from item
        where (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'peru' or i_color = 'metallic') and 
        (i_units = 'Box' or i_units = 'Cup') and
        (i_size = 'N/A' or i_size = 'economy')
        ) or
        (i_category = 'Women' and
        (i_color = 'red' or i_color = 'tan') and
        (i_units = 'Tsp' or i_units = 'Gram') and
        (i_size = 'large' or i_size = 'petite')
        ) or
        (i_category = 'Men' and
        (i_color = 'navajo' or i_color = 'seashell') and
        (i_units = 'Case' or i_units = 'Tbl') and
        (i_size = 'small' or i_size = 'extra large')
        ) or
        (i_category = 'Men' and
        (i_color = 'lavender' or i_color = 'pink') and
        (i_units = 'Bunch' or i_units = 'Lb') and
        (i_size = 'N/A' or i_size = 'economy')
        ))) or
       (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'turquoise' or i_color = 'misty') and 
        (i_units = 'N/A' or i_units = 'Oz') and
        (i_size = 'N/A' or i_size = 'economy')
        ) or
        (i_category = 'Women' and
        (i_color = 'lime' or i_color = 'aquamarine') and
        (i_units = 'Pallet' or i_units = 'Carton') and
        (i_size = 'large' or i_size = 'petite')
        ) or
        (i_category = 'Men' and
        (i_color = 'spring' or i_color = 'dim') and
        (i_units = 'Ounce' or i_units = 'Gross') and
        (i_size = 'small' or i_size = 'extra large')
        ) or
        (i_category = 'Men' and
        (i_color = 'blanched' or i_color = 'burlywood') and
        (i_units = 'Pound' or i_units = 'Unknown') and
        (i_size = 'N/A' or i_size = 'economy')
        )))) > 0
 order by i_product_name
 limit 100;
