-- q41.sql

select  distinct(i_product_name)
 from item i1
 where i_manufact_id between 957 and 957+40 
   and (select count(*) as item_cnt
        from item
        where (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'firebrick' or i_color = 'moccasin') and 
        (i_units = 'Tsp' or i_units = 'Lb') and
        (i_size = 'large' or i_size = 'small')
        ) or
        (i_category = 'Women' and
        (i_color = 'burlywood' or i_color = 'saddle') and
        (i_units = 'Ounce' or i_units = 'Oz') and
        (i_size = 'economy' or i_size = 'petite')
        ) or
        (i_category = 'Men' and
        (i_color = 'blush' or i_color = 'yellow') and
        (i_units = 'Dozen' or i_units = 'Gram') and
        (i_size = 'extra large' or i_size = 'medium')
        ) or
        (i_category = 'Men' and
        (i_color = 'olive' or i_color = 'steel') and
        (i_units = 'Case' or i_units = 'Unknown') and
        (i_size = 'large' or i_size = 'small')
        ))) or
       (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'orange' or i_color = 'sienna') and 
        (i_units = 'Each' or i_units = 'Dram') and
        (i_size = 'large' or i_size = 'small')
        ) or
        (i_category = 'Women' and
        (i_color = 'burnished' or i_color = 'sandy') and
        (i_units = 'Box' or i_units = 'Tbl') and
        (i_size = 'economy' or i_size = 'petite')
        ) or
        (i_category = 'Men' and
        (i_color = 'almond' or i_color = 'rosy') and
        (i_units = 'Carton' or i_units = 'Pallet') and
        (i_size = 'extra large' or i_size = 'medium')
        ) or
        (i_category = 'Men' and
        (i_color = 'red' or i_color = 'coral') and
        (i_units = 'N/A' or i_units = 'Ton') and
        (i_size = 'large' or i_size = 'small')
        )))) > 0
 order by i_product_name
 limit 100;
