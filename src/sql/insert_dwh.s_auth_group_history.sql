INSERT INTO IVIVCHIKYANDEXRU__DWH.s_auth_group_history(hk_l_user_group_activity,
                                   user_id_from,
                                   "event",
                                   event_dt,
                                   load_dt,
                                   load_src)

    SELECT DISTINCT luga.hk_l_user_group_activity,
           user_id_from,
           "event",
           "datetime",
           NOW() load_dt,
           's3' load_src
      FROM IVIVCHIKYANDEXRU__STAGING.group_log gl
 LEFT JOIN IVIVCHIKYANDEXRU__DWH.h_users hu ON gl.user_id = hu.user_id
 LEFT JOIN IVIVCHIKYANDEXRU__DWH.h_groups hg ON gl.group_id = hg.group_id
 LEFT JOIN IVIVCHIKYANDEXRU__DWH.l_user_group_activity luga ON hg.hk_group_id = luga.hk_group_id AND hu.hk_user_id = luga.hk_user_id
     WHERE hk_l_user_group_activity NOT IN (SELECT hk_l_user_group_activity
                                                     FROM IVIVCHIKYANDEXRU__DWH.s_auth_group_history);