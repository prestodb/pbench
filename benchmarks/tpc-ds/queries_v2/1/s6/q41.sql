-- q41.sql

select  distinct(i_product_name)
 from item i1
 where i_manufact_id between 818 and 818+40 
   and (select count(*) as item_cnt
        from item
        where (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'lawn' or i_color = 'misty') and 
        (i_units = 'Unknown' or i_units = 'Tbl') and
        (i_size = 'small' or i_size = 'large')
        ) or
        (i_category = 'Women' and
        (i_color = 'beige' or i_color = 'orange') and
        (i_units = 'Bunch' or i_units = 'N/A') and
        (i_size = 'petite' or i_size = 'economy')
        ) or
        (i_category = 'Men' and
        (i_color = 'light' or i_color = 'royal') and
        (i_units = 'Gram' or i_units = 'Case') and
        (i_size = 'extra large' or i_size = 'N/A')
        ) or
        (i_category = 'Men' and
        (i_color = 'chartreuse' or i_color = 'coral') and
        (i_units = 'Pallet' or i_units = 'Dram') and
        (i_size = 'small' or i_size = 'large')
        ))) or
       (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'peach' or i_color = 'drab') and 
        (i_units = 'Lb' or i_units = 'Cup') and
        (i_size = 'small' or i_size = 'large')
        ) or
        (i_category = 'Women' and
        (i_color = 'papaya' or i_color = 'lavender') and
        (i_units = 'Carton' or i_units = 'Dozen') and
        (i_size = 'petite' or i_size = 'economy')
        ) or
        (i_category = 'Men' and
        (i_color = 'metallic' or i_color = 'indian') and
        (i_units = 'Ton' or i_units = 'Bundle') and
        (i_size = 'extra large' or i_size = 'N/A')
        ) or
        (i_category = 'Men' and
        (i_color = 'almond' or i_color = 'cream') and
        (i_units = 'Gross' or i_units = 'Pound') and
        (i_size = 'small' or i_size = 'large')
        )))) > 0
 order by i_product_name
 limit 100;
