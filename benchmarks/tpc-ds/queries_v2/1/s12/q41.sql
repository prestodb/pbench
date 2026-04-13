-- q41.sql

select  distinct(i_product_name)
 from item i1
 where i_manufact_id between 756 and 756+40 
   and (select count(*) as item_cnt
        from item
        where (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'lemon' or i_color = 'maroon') and 
        (i_units = 'Unknown' or i_units = 'Pallet') and
        (i_size = 'small' or i_size = 'petite')
        ) or
        (i_category = 'Women' and
        (i_color = 'cyan' or i_color = 'navy') and
        (i_units = 'Carton' or i_units = 'Ton') and
        (i_size = 'N/A' or i_size = 'extra large')
        ) or
        (i_category = 'Men' and
        (i_color = 'misty' or i_color = 'green') and
        (i_units = 'Box' or i_units = 'Pound') and
        (i_size = 'medium' or i_size = 'economy')
        ) or
        (i_category = 'Men' and
        (i_color = 'thistle' or i_color = 'azure') and
        (i_units = 'Dozen' or i_units = 'Gross') and
        (i_size = 'small' or i_size = 'petite')
        ))) or
       (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'burnished' or i_color = 'medium') and 
        (i_units = 'Gram' or i_units = 'Tbl') and
        (i_size = 'small' or i_size = 'petite')
        ) or
        (i_category = 'Women' and
        (i_color = 'chartreuse' or i_color = 'grey') and
        (i_units = 'N/A' or i_units = 'Oz') and
        (i_size = 'N/A' or i_size = 'extra large')
        ) or
        (i_category = 'Men' and
        (i_color = 'chocolate' or i_color = 'powder') and
        (i_units = 'Case' or i_units = 'Tsp') and
        (i_size = 'medium' or i_size = 'economy')
        ) or
        (i_category = 'Men' and
        (i_color = 'sky' or i_color = 'antique') and
        (i_units = 'Bunch' or i_units = 'Ounce') and
        (i_size = 'small' or i_size = 'petite')
        )))) > 0
 order by i_product_name
 limit 100;
