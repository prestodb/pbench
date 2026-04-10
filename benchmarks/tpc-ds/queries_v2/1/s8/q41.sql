-- q41.sql

select  distinct(i_product_name)
 from item i1
 where i_manufact_id between 740 and 740+40 
   and (select count(*) as item_cnt
        from item
        where (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'spring' or i_color = 'dim') and 
        (i_units = 'Cup' or i_units = 'Case') and
        (i_size = 'N/A' or i_size = 'extra large')
        ) or
        (i_category = 'Women' and
        (i_color = 'goldenrod' or i_color = 'drab') and
        (i_units = 'Each' or i_units = 'Tsp') and
        (i_size = 'petite' or i_size = 'medium')
        ) or
        (i_category = 'Men' and
        (i_color = 'moccasin' or i_color = 'slate') and
        (i_units = 'Lb' or i_units = 'Dram') and
        (i_size = 'large' or i_size = 'small')
        ) or
        (i_category = 'Men' and
        (i_color = 'chiffon' or i_color = 'cornflower') and
        (i_units = 'Pallet' or i_units = 'Carton') and
        (i_size = 'N/A' or i_size = 'extra large')
        ))) or
       (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'dark' or i_color = 'royal') and 
        (i_units = 'Bundle' or i_units = 'Ounce') and
        (i_size = 'N/A' or i_size = 'extra large')
        ) or
        (i_category = 'Women' and
        (i_color = 'lace' or i_color = 'purple') and
        (i_units = 'Dozen' or i_units = 'Unknown') and
        (i_size = 'petite' or i_size = 'medium')
        ) or
        (i_category = 'Men' and
        (i_color = 'peru' or i_color = 'firebrick') and
        (i_units = 'Ton' or i_units = 'Gram') and
        (i_size = 'large' or i_size = 'small')
        ) or
        (i_category = 'Men' and
        (i_color = 'magenta' or i_color = 'ivory') and
        (i_units = 'Bunch' or i_units = 'Gross') and
        (i_size = 'N/A' or i_size = 'extra large')
        )))) > 0
 order by i_product_name
 limit 100;
