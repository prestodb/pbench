-- q41.sql

select  distinct(i_product_name)
 from item i1
 where i_manufact_id between 705 and 705+40 
   and (select count(*) as item_cnt
        from item
        where (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'brown' or i_color = 'metallic') and 
        (i_units = 'Bunch' or i_units = 'Case') and
        (i_size = 'extra large' or i_size = 'large')
        ) or
        (i_category = 'Women' and
        (i_color = 'wheat' or i_color = 'olive') and
        (i_units = 'Lb' or i_units = 'Cup') and
        (i_size = 'small' or i_size = 'economy')
        ) or
        (i_category = 'Men' and
        (i_color = 'sienna' or i_color = 'cornflower') and
        (i_units = 'Bundle' or i_units = 'Pallet') and
        (i_size = 'N/A' or i_size = 'medium')
        ) or
        (i_category = 'Men' and
        (i_color = 'powder' or i_color = 'floral') and
        (i_units = 'Ounce' or i_units = 'Dozen') and
        (i_size = 'extra large' or i_size = 'large')
        ))) or
       (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'lavender' or i_color = 'steel') and 
        (i_units = 'Unknown' or i_units = 'Carton') and
        (i_size = 'extra large' or i_size = 'large')
        ) or
        (i_category = 'Women' and
        (i_color = 'mint' or i_color = 'snow') and
        (i_units = 'Tbl' or i_units = 'Ton') and
        (i_size = 'small' or i_size = 'economy')
        ) or
        (i_category = 'Men' and
        (i_color = 'bisque' or i_color = 'green') and
        (i_units = 'Gross' or i_units = 'Each') and
        (i_size = 'N/A' or i_size = 'medium')
        ) or
        (i_category = 'Men' and
        (i_color = 'papaya' or i_color = 'thistle') and
        (i_units = 'N/A' or i_units = 'Pound') and
        (i_size = 'extra large' or i_size = 'large')
        )))) > 0
 order by i_product_name
 limit 100;
