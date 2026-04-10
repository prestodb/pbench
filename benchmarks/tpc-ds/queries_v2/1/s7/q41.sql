-- q41.sql

select  distinct(i_product_name)
 from item i1
 where i_manufact_id between 920 and 920+40 
   and (select count(*) as item_cnt
        from item
        where (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'dark' or i_color = 'medium') and 
        (i_units = 'Ounce' or i_units = 'Bundle') and
        (i_size = 'small' or i_size = 'economy')
        ) or
        (i_category = 'Women' and
        (i_color = 'white' or i_color = 'bisque') and
        (i_units = 'Cup' or i_units = 'Tbl') and
        (i_size = 'N/A' or i_size = 'extra large')
        ) or
        (i_category = 'Men' and
        (i_color = 'papaya' or i_color = 'olive') and
        (i_units = 'Oz' or i_units = 'Tsp') and
        (i_size = 'medium' or i_size = 'petite')
        ) or
        (i_category = 'Men' and
        (i_color = 'misty' or i_color = 'magenta') and
        (i_units = 'Dozen' or i_units = 'Pallet') and
        (i_size = 'small' or i_size = 'economy')
        ))) or
       (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'drab' or i_color = 'beige') and 
        (i_units = 'Carton' or i_units = 'Box') and
        (i_size = 'small' or i_size = 'economy')
        ) or
        (i_category = 'Women' and
        (i_color = 'lace' or i_color = 'tomato') and
        (i_units = 'Bunch' or i_units = 'Dram') and
        (i_size = 'N/A' or i_size = 'extra large')
        ) or
        (i_category = 'Men' and
        (i_color = 'dim' or i_color = 'blush') and
        (i_units = 'Unknown' or i_units = 'Ton') and
        (i_size = 'medium' or i_size = 'petite')
        ) or
        (i_category = 'Men' and
        (i_color = 'cream' or i_color = 'aquamarine') and
        (i_units = 'Each' or i_units = 'Case') and
        (i_size = 'small' or i_size = 'economy')
        )))) > 0
 order by i_product_name
 limit 100;
