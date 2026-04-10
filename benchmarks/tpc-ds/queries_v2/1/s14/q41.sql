-- q41.sql

select  distinct(i_product_name)
 from item i1
 where i_manufact_id between 788 and 788+40 
   and (select count(*) as item_cnt
        from item
        where (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'blanched' or i_color = 'orchid') and 
        (i_units = 'Gross' or i_units = 'Dozen') and
        (i_size = 'small' or i_size = 'economy')
        ) or
        (i_category = 'Women' and
        (i_color = 'chiffon' or i_color = 'violet') and
        (i_units = 'Unknown' or i_units = 'Lb') and
        (i_size = 'extra large' or i_size = 'large')
        ) or
        (i_category = 'Men' and
        (i_color = 'cornflower' or i_color = 'honeydew') and
        (i_units = 'N/A' or i_units = 'Ounce') and
        (i_size = 'medium' or i_size = 'petite')
        ) or
        (i_category = 'Men' and
        (i_color = 'olive' or i_color = 'ivory') and
        (i_units = 'Box' or i_units = 'Carton') and
        (i_size = 'small' or i_size = 'economy')
        ))) or
       (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'dim' or i_color = 'papaya') and 
        (i_units = 'Gram' or i_units = 'Each') and
        (i_size = 'small' or i_size = 'economy')
        ) or
        (i_category = 'Women' and
        (i_color = 'metallic' or i_color = 'seashell') and
        (i_units = 'Oz' or i_units = 'Ton') and
        (i_size = 'extra large' or i_size = 'large')
        ) or
        (i_category = 'Men' and
        (i_color = 'sandy' or i_color = 'chocolate') and
        (i_units = 'Bundle' or i_units = 'Case') and
        (i_size = 'medium' or i_size = 'petite')
        ) or
        (i_category = 'Men' and
        (i_color = 'blush' or i_color = 'indian') and
        (i_units = 'Cup' or i_units = 'Bunch') and
        (i_size = 'small' or i_size = 'economy')
        )))) > 0
 order by i_product_name
 limit 100;
