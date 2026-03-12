-- q41.sql

select  distinct(i_product_name)
 from item i1
 where i_manufact_id between 898 and 898+40 
   and (select count(*) as item_cnt
        from item
        where (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'sky' or i_color = 'blush') and 
        (i_units = 'Dozen' or i_units = 'Pallet') and
        (i_size = 'petite' or i_size = 'small')
        ) or
        (i_category = 'Women' and
        (i_color = 'salmon' or i_color = 'grey') and
        (i_units = 'Each' or i_units = 'Dram') and
        (i_size = 'economy' or i_size = 'extra large')
        ) or
        (i_category = 'Men' and
        (i_color = 'medium' or i_color = 'floral') and
        (i_units = 'Oz' or i_units = 'Ton') and
        (i_size = 'N/A' or i_size = 'large')
        ) or
        (i_category = 'Men' and
        (i_color = 'red' or i_color = 'maroon') and
        (i_units = 'Carton' or i_units = 'Gram') and
        (i_size = 'petite' or i_size = 'small')
        ))) or
       (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'bisque' or i_color = 'steel') and 
        (i_units = 'Case' or i_units = 'Bundle') and
        (i_size = 'petite' or i_size = 'small')
        ) or
        (i_category = 'Women' and
        (i_color = 'dark' or i_color = 'cornsilk') and
        (i_units = 'N/A' or i_units = 'Box') and
        (i_size = 'economy' or i_size = 'extra large')
        ) or
        (i_category = 'Men' and
        (i_color = 'brown' or i_color = 'slate') and
        (i_units = 'Unknown' or i_units = 'Gross') and
        (i_size = 'N/A' or i_size = 'large')
        ) or
        (i_category = 'Men' and
        (i_color = 'almond' or i_color = 'hot') and
        (i_units = 'Bunch' or i_units = 'Pound') and
        (i_size = 'petite' or i_size = 'small')
        )))) > 0
 order by i_product_name
 limit 100;
