-- q41.sql

select  distinct(i_product_name)
 from item i1
 where i_manufact_id between 879 and 879+40 
   and (select count(*) as item_cnt
        from item
        where (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'dark' or i_color = 'bisque') and 
        (i_units = 'Bundle' or i_units = 'Tbl') and
        (i_size = 'large' or i_size = 'medium')
        ) or
        (i_category = 'Women' and
        (i_color = 'floral' or i_color = 'dodger') and
        (i_units = 'Gross' or i_units = 'Case') and
        (i_size = 'extra large' or i_size = 'small')
        ) or
        (i_category = 'Men' and
        (i_color = 'antique' or i_color = 'puff') and
        (i_units = 'Dram' or i_units = 'Carton') and
        (i_size = 'economy' or i_size = 'petite')
        ) or
        (i_category = 'Men' and
        (i_color = 'azure' or i_color = 'thistle') and
        (i_units = 'Bunch' or i_units = 'Unknown') and
        (i_size = 'large' or i_size = 'medium')
        ))) or
       (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'snow' or i_color = 'linen') and 
        (i_units = 'Ton' or i_units = 'Box') and
        (i_size = 'large' or i_size = 'medium')
        ) or
        (i_category = 'Women' and
        (i_color = 'pink' or i_color = 'navy') and
        (i_units = 'Dozen' or i_units = 'Tsp') and
        (i_size = 'extra large' or i_size = 'small')
        ) or
        (i_category = 'Men' and
        (i_color = 'rosy' or i_color = 'moccasin') and
        (i_units = 'Each' or i_units = 'N/A') and
        (i_size = 'economy' or i_size = 'petite')
        ) or
        (i_category = 'Men' and
        (i_color = 'midnight' or i_color = 'olive') and
        (i_units = 'Pallet' or i_units = 'Cup') and
        (i_size = 'large' or i_size = 'medium')
        )))) > 0
 order by i_product_name
 limit 100;
