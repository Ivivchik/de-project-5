INSERT INTO IVIVCHIKYANDEXRU__DWH.h_groups(hk_group_id,
                                           group_id,
                                           registration_dt,
                                           load_dt,
                                           load_src)
    SELECT DISTINCT HASH(id) hk_group_id,
           id group_id,
           registration_dt,
           NOW() load_dt,
           's3' load_src
      FROM IVIVCHIKYANDEXRU__STAGING.groups
     WHERE HASH(id) NOT IN (SELECT hk_group_id
                              FROM IVIVCHIKYANDEXRU__DWH.h_groups);