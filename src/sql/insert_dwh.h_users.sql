INSERT INTO IVIVCHIKYANDEXRU__DWH.h_users(hk_user_id,
                                          user_id,
                                          registration_dt,
                                          load_dt,
                                          load_src)
    SELECT DISTINCT HASH(id) hk_user_id,
           id user_id,
           registration_dt,
           NOW() load_dt,
           's3' load_src
      FROM IVIVCHIKYANDEXRU__STAGING.users
     WHERE HASH(id) NOT IN (SELECT hk_user_id
                              FROM IVIVCHIKYANDEXRU__DWH.h_users);