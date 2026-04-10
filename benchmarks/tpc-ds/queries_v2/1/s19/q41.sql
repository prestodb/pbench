-- q41.sql

select  distinct(i_product_name)
 from item i1
 where i_manufact_id between 724 and 724+40 
   and (select count(*) as item_cnt
        from item
        where (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'dim' or i_color = 'rosy') and 
        (i_units = 'Bunch' or i_units = 'Case') and
        (i_size = 'medium' or i_size = 'extra large')
        ) or
        (i_category = 'Women' and
        (i_color = 'ghost' or i_color = 'khaki') and
        (i_units = 'Unknown' or i_units = 'N/A') and
        (i_size = 'economy' or i_size = 'small')
        ) or
        (i_category = 'Men' and
        (i_color = 'linen' or i_color = 'navy') and
        (i_units = 'Ounce' or i_units = 'Carton') and
        (i_size = 'large' or i_size = 'petite')
        ) or
        (i_category = 'Men' and
        (i_color = 'snow' or i_color = 'chiffon') and
        (i_units = 'Lb' or i_units = 'Gross') and
        (i_size = 'medium' or i_size = 'extra large')
        ))) or
       (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'peru' or i_color = 'wheat') and 
        (i_units = 'Gram' or i_units = 'Pallet') and
        (i_size = 'medium' or i_size = 'extra large')
        ) or
        (i_category = 'Women' and
        (i_color = 'hot' or i_color = 'tan') and
        (i_units = 'Bundle' or i_units = 'Tsp') and
        (i_size = 'economy' or i_size = 'small')
        ) or
        (i_category = 'Men' and
        (i_color = 'yellow' or i_color = 'almond') and
        (i_units = 'Cup' or i_units = 'Dozen') and
        (i_size = 'large' or i_size = 'petite')
        ) or
        (i_category = 'Men' and
        (i_color = 'beige' or i_color = 'coral') and
        (i_units = 'Each' or i_units = 'Pound') and
        (i_size = 'medium' or i_size = 'extra large')
        )))) > 0
 order by i_product_name
 limit 100;
