INSERT INTO IVIVCHIKYANDEXRU__DWH.l_user_group_activity(hk_l_user_group_activity,
                                                        hk_user_id,
                                                        hk_group_id,
                                                        load_dt,
                                                        load_src)
    SELECT DISTINCT HASH(hu.hk_user_id,hg.hk_group_id),
           hu.hk_user_id,
           hg.hk_group_id,
           NOW() load_dt,
           's3' load_src
      FROM IVIVCHIKYANDEXRU__STAGING.group_log gl
 LEFT JOIN IVIVCHIKYANDEXRU__DWH.h_users hu ON gl.user_id = hu.user_id
 LEFT JOIN IVIVCHIKYANDEXRU__DWH.h_groups hg ON gl.group_id = hg.group_id
WHERE HASH (hu.hk_user_id, hg.hk_group_id) NOT IN (SELECT hk_l_user_group_activity
                                                     FROM IVIVCHIKYANDEXRU__DWH.l_user_group_activity);