-- q41.sql

select  distinct(i_product_name)
 from item i1
 where i_manufact_id between 733 and 733+40 
   and (select count(*) as item_cnt
        from item
        where (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'azure' or i_color = 'blush') and 
        (i_units = 'Ounce' or i_units = 'Each') and
        (i_size = 'economy' or i_size = 'medium')
        ) or
        (i_category = 'Women' and
        (i_color = 'peru' or i_color = 'hot') and
        (i_units = 'N/A' or i_units = 'Lb') and
        (i_size = 'large' or i_size = 'petite')
        ) or
        (i_category = 'Men' and
        (i_color = 'pink' or i_color = 'floral') and
        (i_units = 'Bundle' or i_units = 'Bunch') and
        (i_size = 'N/A' or i_size = 'extra large')
        ) or
        (i_category = 'Men' and
        (i_color = 'linen' or i_color = 'black') and
        (i_units = 'Oz' or i_units = 'Case') and
        (i_size = 'economy' or i_size = 'medium')
        ))) or
       (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'white' or i_color = 'dim') and 
        (i_units = 'Carton' or i_units = 'Dozen') and
        (i_size = 'economy' or i_size = 'medium')
        ) or
        (i_category = 'Women' and
        (i_color = 'drab' or i_color = 'powder') and
        (i_units = 'Unknown' or i_units = 'Pallet') and
        (i_size = 'large' or i_size = 'petite')
        ) or
        (i_category = 'Men' and
        (i_color = 'honeydew' or i_color = 'wheat') and
        (i_units = 'Dram' or i_units = 'Tbl') and
        (i_size = 'N/A' or i_size = 'extra large')
        ) or
        (i_category = 'Men' and
        (i_color = 'seashell' or i_color = 'frosted') and
        (i_units = 'Gram' or i_units = 'Gross') and
        (i_size = 'economy' or i_size = 'medium')
        )))) > 0
 order by i_product_name
 limit 100;
