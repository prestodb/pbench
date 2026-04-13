-- q41.sql

select  distinct(i_product_name)
 from item i1
 where i_manufact_id between 940 and 940+40 
   and (select count(*) as item_cnt
        from item
        where (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'puff' or i_color = 'firebrick') and 
        (i_units = 'Ounce' or i_units = 'Ton') and
        (i_size = 'economy' or i_size = 'extra large')
        ) or
        (i_category = 'Women' and
        (i_color = 'lime' or i_color = 'almond') and
        (i_units = 'Each' or i_units = 'Cup') and
        (i_size = 'small' or i_size = 'large')
        ) or
        (i_category = 'Men' and
        (i_color = 'tan' or i_color = 'medium') and
        (i_units = 'Case' or i_units = 'Gross') and
        (i_size = 'N/A' or i_size = 'petite')
        ) or
        (i_category = 'Men' and
        (i_color = 'moccasin' or i_color = 'chiffon') and
        (i_units = 'Dozen' or i_units = 'Tbl') and
        (i_size = 'economy' or i_size = 'extra large')
        ))) or
       (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'snow' or i_color = 'burlywood') and 
        (i_units = 'Bunch' or i_units = 'Lb') and
        (i_size = 'economy' or i_size = 'extra large')
        ) or
        (i_category = 'Women' and
        (i_color = 'papaya' or i_color = 'orange') and
        (i_units = 'Bundle' or i_units = 'Pallet') and
        (i_size = 'small' or i_size = 'large')
        ) or
        (i_category = 'Men' and
        (i_color = 'deep' or i_color = 'misty') and
        (i_units = 'Tsp' or i_units = 'Carton') and
        (i_size = 'N/A' or i_size = 'petite')
        ) or
        (i_category = 'Men' and
        (i_color = 'blush' or i_color = 'cream') and
        (i_units = 'Pound' or i_units = 'Box') and
        (i_size = 'economy' or i_size = 'extra large')
        )))) > 0
 order by i_product_name
 limit 100;
