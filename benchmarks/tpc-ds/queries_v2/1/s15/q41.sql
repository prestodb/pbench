-- q41.sql

select  distinct(i_product_name)
 from item i1
 where i_manufact_id between 811 and 811+40 
   and (select count(*) as item_cnt
        from item
        where (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'lemon' or i_color = 'medium') and 
        (i_units = 'Pallet' or i_units = 'Unknown') and
        (i_size = 'medium' or i_size = 'extra large')
        ) or
        (i_category = 'Women' and
        (i_color = 'coral' or i_color = 'turquoise') and
        (i_units = 'Tbl' or i_units = 'Lb') and
        (i_size = 'large' or i_size = 'petite')
        ) or
        (i_category = 'Men' and
        (i_color = 'thistle' or i_color = 'metallic') and
        (i_units = 'Box' or i_units = 'Gross') and
        (i_size = 'small' or i_size = 'N/A')
        ) or
        (i_category = 'Men' and
        (i_color = 'dim' or i_color = 'red') and
        (i_units = 'Case' or i_units = 'Tsp') and
        (i_size = 'medium' or i_size = 'extra large')
        ))) or
       (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'maroon' or i_color = 'dark') and 
        (i_units = 'Dram' or i_units = 'N/A') and
        (i_size = 'medium' or i_size = 'extra large')
        ) or
        (i_category = 'Women' and
        (i_color = 'light' or i_color = 'seashell') and
        (i_units = 'Each' or i_units = 'Pound') and
        (i_size = 'large' or i_size = 'petite')
        ) or
        (i_category = 'Men' and
        (i_color = 'sky' or i_color = 'powder') and
        (i_units = 'Cup' or i_units = 'Bunch') and
        (i_size = 'small' or i_size = 'N/A')
        ) or
        (i_category = 'Men' and
        (i_color = 'orange' or i_color = 'grey') and
        (i_units = 'Ton' or i_units = 'Dozen') and
        (i_size = 'medium' or i_size = 'extra large')
        )))) > 0
 order by i_product_name
 limit 100;
