-- q41.sql

select  distinct(i_product_name)
 from item i1
 where i_manufact_id between 735 and 735+40 
   and (select count(*) as item_cnt
        from item
        where (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'light' or i_color = 'dark') and 
        (i_units = 'Lb' or i_units = 'Case') and
        (i_size = 'petite' or i_size = 'small')
        ) or
        (i_category = 'Women' and
        (i_color = 'hot' or i_color = 'brown') and
        (i_units = 'Pallet' or i_units = 'Ounce') and
        (i_size = 'large' or i_size = 'medium')
        ) or
        (i_category = 'Men' and
        (i_color = 'azure' or i_color = 'salmon') and
        (i_units = 'Dozen' or i_units = 'Dram') and
        (i_size = 'N/A' or i_size = 'economy')
        ) or
        (i_category = 'Men' and
        (i_color = 'blush' or i_color = 'purple') and
        (i_units = 'Unknown' or i_units = 'N/A') and
        (i_size = 'petite' or i_size = 'small')
        ))) or
       (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'sienna' or i_color = 'ivory') and 
        (i_units = 'Bunch' or i_units = 'Carton') and
        (i_size = 'petite' or i_size = 'small')
        ) or
        (i_category = 'Women' and
        (i_color = 'pale' or i_color = 'midnight') and
        (i_units = 'Bundle' or i_units = 'Ton') and
        (i_size = 'large' or i_size = 'medium')
        ) or
        (i_category = 'Men' and
        (i_color = 'puff' or i_color = 'mint') and
        (i_units = 'Oz' or i_units = 'Box') and
        (i_size = 'N/A' or i_size = 'economy')
        ) or
        (i_category = 'Men' and
        (i_color = 'honeydew' or i_color = 'navajo') and
        (i_units = 'Tsp' or i_units = 'Cup') and
        (i_size = 'petite' or i_size = 'small')
        )))) > 0
 order by i_product_name
 limit 100;
