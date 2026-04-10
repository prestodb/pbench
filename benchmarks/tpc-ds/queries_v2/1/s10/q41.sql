-- q41.sql

select  distinct(i_product_name)
 from item i1
 where i_manufact_id between 973 and 973+40 
   and (select count(*) as item_cnt
        from item
        where (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'aquamarine' or i_color = 'orchid') and 
        (i_units = 'Oz' or i_units = 'Each') and
        (i_size = 'petite' or i_size = 'N/A')
        ) or
        (i_category = 'Women' and
        (i_color = 'maroon' or i_color = 'midnight') and
        (i_units = 'Bundle' or i_units = 'Ounce') and
        (i_size = 'extra large' or i_size = 'economy')
        ) or
        (i_category = 'Men' and
        (i_color = 'lace' or i_color = 'royal') and
        (i_units = 'Box' or i_units = 'Dozen') and
        (i_size = 'large' or i_size = 'medium')
        ) or
        (i_category = 'Men' and
        (i_color = 'slate' or i_color = 'indian') and
        (i_units = 'Dram' or i_units = 'Pound') and
        (i_size = 'petite' or i_size = 'N/A')
        ))) or
       (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'frosted' or i_color = 'navajo') and 
        (i_units = 'Lb' or i_units = 'Carton') and
        (i_size = 'petite' or i_size = 'N/A')
        ) or
        (i_category = 'Women' and
        (i_color = 'dark' or i_color = 'khaki') and
        (i_units = 'Gram' or i_units = 'Ton') and
        (i_size = 'extra large' or i_size = 'economy')
        ) or
        (i_category = 'Men' and
        (i_color = 'dodger' or i_color = 'grey') and
        (i_units = 'Unknown' or i_units = 'Pallet') and
        (i_size = 'large' or i_size = 'medium')
        ) or
        (i_category = 'Men' and
        (i_color = 'cyan' or i_color = 'sky') and
        (i_units = 'Case' or i_units = 'Cup') and
        (i_size = 'petite' or i_size = 'N/A')
        )))) > 0
 order by i_product_name
 limit 100;
