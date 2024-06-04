-- Sample
WITH fleet_car_tag_list AS (SELECT
        fc.id AS car_id
        ,ARRAY_JOIN(ARRAY_AGG(DISTINCT fct.id ORDER BY fct.id),';') AS car_tag_id_list
        ,ARRAY_JOIN(ARRAY_AGG(DISTINCT fct.key ORDER BY fct.key),';') AS car_tag_key_list
        ,ARRAY_JOIN(ARRAY_AGG(DISTINCT fct.name ORDER BY fct.name),';') AS car_tag_list

      FROM ng_public.fleet_car fc
      LEFT JOIN ng_public.fleet_car_tag_binding fctb
        ON fctb.car_id = fc.id
      LEFT JOIN ng_public.fleet_car_tag fct
        ON fct.id = fctb.car_tag_id

      ---Deleted/removed tags are excluded (a query for finding deleted rows is state = 'deleted' OR state IS NULL).---
      WHERE
        LOWER(fctb.state) = 'active'

      GROUP BY
        fc.id
      )
SELECT
    branded_car_enrollment.car_id  AS \"branded_car_enrollment.target_id\"
FROM lks.LR_branded_car_enrollment AS branded_car_enrollment
LEFT JOIN admin_system_city  AS admin_system_city ON branded_car_enrollment.city_id = admin_system_city.id
LEFT JOIN lks.LR_admin_system_country AS admin_system_country ON branded_car_enrollment.country = admin_system_country.code
LEFT JOIN fleet_car_tag_list ON branded_car_enrollment.car_id =  fleet_car_tag_list.car_id
WHERE ((fleet_car_tag_list.car_tag_list ) NOT LIKE '%bajaji%' AND ((fleet_car_tag_list.car_tag_list ) NOT LIKE '%Bajaji%' AND (fleet_car_tag_list.car_tag_list ) NOT LIKE '%Boda%') AND ((fleet_car_tag_list.car_tag_list ) NOT LIKE '%Bodas%' AND (fleet_car_tag_list.car_tag_list ) NOT LIKE '%boda%' AND ((fleet_car_tag_list.car_tag_list ) NOT LIKE '%bodas%' AND (fleet_car_tag_list.car_tag_list ) NOT LIKE '%TukTuk%')) OR (fleet_car_tag_list.car_tag_list ) IS NULL) AND (((( branded_car_enrollment.car_id  >= 1) AND ( branded_car_enrollment.country  = 'gh')) AND ( branded_car_enrollment.cohort  = 'Branded_Verified')) AND ( branded_car_enrollment.city_id  = 991))
GROUP BY
    1
ORDER BY
    1
LIMIT 5000
