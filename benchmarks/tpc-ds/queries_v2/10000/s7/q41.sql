-- q41.sql

select  distinct(i_product_name)
 from item i1
 where i_manufact_id between 842 and 842+40 
   and (select count(*) as item_cnt
        from item
        where (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'honeydew' or i_color = 'peru') and 
        (i_units = 'Tsp' or i_units = 'Case') and
        (i_size = 'extra large' or i_size = 'large')
        ) or
        (i_category = 'Women' and
        (i_color = 'lace' or i_color = 'pink') and
        (i_units = 'Ton' or i_units = 'Gram') and
        (i_size = 'economy' or i_size = 'N/A')
        ) or
        (i_category = 'Men' and
        (i_color = 'red' or i_color = 'cornsilk') and
        (i_units = 'N/A' or i_units = 'Box') and
        (i_size = 'medium' or i_size = 'petite')
        ) or
        (i_category = 'Men' and
        (i_color = 'cyan' or i_color = 'ivory') and
        (i_units = 'Cup' or i_units = 'Pound') and
        (i_size = 'extra large' or i_size = 'large')
        ))) or
       (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'cream' or i_color = 'hot') and 
        (i_units = 'Pallet' or i_units = 'Bunch') and
        (i_size = 'extra large' or i_size = 'large')
        ) or
        (i_category = 'Women' and
        (i_color = 'olive' or i_color = 'floral') and
        (i_units = 'Bundle' or i_units = 'Carton') and
        (i_size = 'economy' or i_size = 'N/A')
        ) or
        (i_category = 'Men' and
        (i_color = 'papaya' or i_color = 'green') and
        (i_units = 'Oz' or i_units = 'Dozen') and
        (i_size = 'medium' or i_size = 'petite')
        ) or
        (i_category = 'Men' and
        (i_color = 'dim' or i_color = 'wheat') and
        (i_units = 'Each' or i_units = 'Dram') and
        (i_size = 'extra large' or i_size = 'large')
        )))) > 0
 order by i_product_name
 limit 100;
