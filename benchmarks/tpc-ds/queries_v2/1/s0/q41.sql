-- q41.sql

select  distinct(i_product_name)
 from item i1
 where i_manufact_id between 857 and 857+40 
   and (select count(*) as item_cnt
        from item
        where (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'green' or i_color = 'yellow') and 
        (i_units = 'Pallet' or i_units = 'Each') and
        (i_size = 'petite' or i_size = 'medium')
        ) or
        (i_category = 'Women' and
        (i_color = 'magenta' or i_color = 'lime') and
        (i_units = 'Cup' or i_units = 'Case') and
        (i_size = 'large' or i_size = 'economy')
        ) or
        (i_category = 'Men' and
        (i_color = 'black' or i_color = 'forest') and
        (i_units = 'Tsp' or i_units = 'N/A') and
        (i_size = 'small' or i_size = 'N/A')
        ) or
        (i_category = 'Men' and
        (i_color = 'seashell' or i_color = 'white') and
        (i_units = 'Box' or i_units = 'Oz') and
        (i_size = 'petite' or i_size = 'medium')
        ))) or
       (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'coral' or i_color = 'dark') and 
        (i_units = 'Carton' or i_units = 'Lb') and
        (i_size = 'petite' or i_size = 'medium')
        ) or
        (i_category = 'Women' and
        (i_color = 'chartreuse' or i_color = 'brown') and
        (i_units = 'Ounce' or i_units = 'Gross') and
        (i_size = 'large' or i_size = 'economy')
        ) or
        (i_category = 'Men' and
        (i_color = 'gainsboro' or i_color = 'peach') and
        (i_units = 'Bundle' or i_units = 'Unknown') and
        (i_size = 'small' or i_size = 'N/A')
        ) or
        (i_category = 'Men' and
        (i_color = 'cyan' or i_color = 'navajo') and
        (i_units = 'Bunch' or i_units = 'Gram') and
        (i_size = 'petite' or i_size = 'medium')
        )))) > 0
 order by i_product_name
 limit 100;
