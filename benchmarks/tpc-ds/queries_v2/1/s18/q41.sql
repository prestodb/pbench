-- q41.sql

select  distinct(i_product_name)
 from item i1
 where i_manufact_id between 982 and 982+40 
   and (select count(*) as item_cnt
        from item
        where (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'medium' or i_color = 'cornsilk') and 
        (i_units = 'Tsp' or i_units = 'Ton') and
        (i_size = 'N/A' or i_size = 'large')
        ) or
        (i_category = 'Women' and
        (i_color = 'ghost' or i_color = 'royal') and
        (i_units = 'Pound' or i_units = 'Dram') and
        (i_size = 'small' or i_size = 'petite')
        ) or
        (i_category = 'Men' and
        (i_color = 'blanched' or i_color = 'indian') and
        (i_units = 'Dozen' or i_units = 'Cup') and
        (i_size = 'medium' or i_size = 'economy')
        ) or
        (i_category = 'Men' and
        (i_color = 'antique' or i_color = 'red') and
        (i_units = 'Gram' or i_units = 'Ounce') and
        (i_size = 'N/A' or i_size = 'large')
        ))) or
       (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'smoke' or i_color = 'blue') and 
        (i_units = 'Box' or i_units = 'N/A') and
        (i_size = 'N/A' or i_size = 'large')
        ) or
        (i_category = 'Women' and
        (i_color = 'cornflower' or i_color = 'peru') and
        (i_units = 'Bunch' or i_units = 'Lb') and
        (i_size = 'small' or i_size = 'petite')
        ) or
        (i_category = 'Men' and
        (i_color = 'rosy' or i_color = 'burnished') and
        (i_units = 'Each' or i_units = 'Gross') and
        (i_size = 'medium' or i_size = 'economy')
        ) or
        (i_category = 'Men' and
        (i_color = 'slate' or i_color = 'spring') and
        (i_units = 'Case' or i_units = 'Carton') and
        (i_size = 'N/A' or i_size = 'large')
        )))) > 0
 order by i_product_name
 limit 100;
