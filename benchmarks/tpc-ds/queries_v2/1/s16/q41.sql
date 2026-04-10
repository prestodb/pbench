-- q41.sql

select  distinct(i_product_name)
 from item i1
 where i_manufact_id between 968 and 968+40 
   and (select count(*) as item_cnt
        from item
        where (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'white' or i_color = 'snow') and 
        (i_units = 'Bunch' or i_units = 'Pound') and
        (i_size = 'economy' or i_size = 'petite')
        ) or
        (i_category = 'Women' and
        (i_color = 'midnight' or i_color = 'tomato') and
        (i_units = 'Carton' or i_units = 'Dram') and
        (i_size = 'small' or i_size = 'N/A')
        ) or
        (i_category = 'Men' and
        (i_color = 'dim' or i_color = 'metallic') and
        (i_units = 'Dozen' or i_units = 'Lb') and
        (i_size = 'large' or i_size = 'medium')
        ) or
        (i_category = 'Men' and
        (i_color = 'almond' or i_color = 'chiffon') and
        (i_units = 'Bundle' or i_units = 'Cup') and
        (i_size = 'economy' or i_size = 'petite')
        ))) or
       (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'lime' or i_color = 'peru') and 
        (i_units = 'Each' or i_units = 'Unknown') and
        (i_size = 'economy' or i_size = 'petite')
        ) or
        (i_category = 'Women' and
        (i_color = 'burlywood' or i_color = 'gainsboro') and
        (i_units = 'Gross' or i_units = 'Tbl') and
        (i_size = 'small' or i_size = 'N/A')
        ) or
        (i_category = 'Men' and
        (i_color = 'firebrick' or i_color = 'maroon') and
        (i_units = 'Oz' or i_units = 'Ton') and
        (i_size = 'large' or i_size = 'medium')
        ) or
        (i_category = 'Men' and
        (i_color = 'salmon' or i_color = 'azure') and
        (i_units = 'Box' or i_units = 'Tsp') and
        (i_size = 'economy' or i_size = 'petite')
        )))) > 0
 order by i_product_name
 limit 100;
