-- q41.sql

select  distinct(i_product_name)
 from item i1
 where i_manufact_id between 746 and 746+40 
   and (select count(*) as item_cnt
        from item
        where (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'magenta' or i_color = 'aquamarine') and 
        (i_units = 'Oz' or i_units = 'Dram') and
        (i_size = 'small' or i_size = 'economy')
        ) or
        (i_category = 'Women' and
        (i_color = 'antique' or i_color = 'honeydew') and
        (i_units = 'Pallet' or i_units = 'Bunch') and
        (i_size = 'large' or i_size = 'extra large')
        ) or
        (i_category = 'Men' and
        (i_color = 'dark' or i_color = 'dim') and
        (i_units = 'Each' or i_units = 'Dozen') and
        (i_size = 'medium' or i_size = 'N/A')
        ) or
        (i_category = 'Men' and
        (i_color = 'purple' or i_color = 'plum') and
        (i_units = 'Tsp' or i_units = 'Cup') and
        (i_size = 'small' or i_size = 'economy')
        ))) or
       (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'cream' or i_color = 'chiffon') and 
        (i_units = 'Unknown' or i_units = 'Ton') and
        (i_size = 'small' or i_size = 'economy')
        ) or
        (i_category = 'Women' and
        (i_color = 'lime' or i_color = 'cornsilk') and
        (i_units = 'Gross' or i_units = 'Box') and
        (i_size = 'large' or i_size = 'extra large')
        ) or
        (i_category = 'Men' and
        (i_color = 'green' or i_color = 'blush') and
        (i_units = 'Carton' or i_units = 'Pound') and
        (i_size = 'medium' or i_size = 'N/A')
        ) or
        (i_category = 'Men' and
        (i_color = 'floral' or i_color = 'red') and
        (i_units = 'Case' or i_units = 'Gram') and
        (i_size = 'small' or i_size = 'economy')
        )))) > 0
 order by i_product_name
 limit 100;
