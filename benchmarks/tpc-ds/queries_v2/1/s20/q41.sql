-- q41.sql

select  distinct(i_product_name)
 from item i1
 where i_manufact_id between 939 and 939+40 
   and (select count(*) as item_cnt
        from item
        where (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'violet' or i_color = 'goldenrod') and 
        (i_units = 'Pallet' or i_units = 'Gram') and
        (i_size = 'small' or i_size = 'extra large')
        ) or
        (i_category = 'Women' and
        (i_color = 'medium' or i_color = 'yellow') and
        (i_units = 'Bunch' or i_units = 'Pound') and
        (i_size = 'large' or i_size = 'N/A')
        ) or
        (i_category = 'Men' and
        (i_color = 'blanched' or i_color = 'royal') and
        (i_units = 'Cup' or i_units = 'Oz') and
        (i_size = 'petite' or i_size = 'economy')
        ) or
        (i_category = 'Men' and
        (i_color = 'peru' or i_color = 'brown') and
        (i_units = 'Lb' or i_units = 'Dram') and
        (i_size = 'small' or i_size = 'extra large')
        ))) or
       (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'firebrick' or i_color = 'frosted') and 
        (i_units = 'Unknown' or i_units = 'Tsp') and
        (i_size = 'small' or i_size = 'extra large')
        ) or
        (i_category = 'Women' and
        (i_color = 'bisque' or i_color = 'lavender') and
        (i_units = 'N/A' or i_units = 'Ounce') and
        (i_size = 'large' or i_size = 'N/A')
        ) or
        (i_category = 'Men' and
        (i_color = 'burlywood' or i_color = 'green') and
        (i_units = 'Carton' or i_units = 'Ton') and
        (i_size = 'petite' or i_size = 'economy')
        ) or
        (i_category = 'Men' and
        (i_color = 'burnished' or i_color = 'linen') and
        (i_units = 'Dozen' or i_units = 'Bundle') and
        (i_size = 'small' or i_size = 'extra large')
        )))) > 0
 order by i_product_name
 limit 100;
