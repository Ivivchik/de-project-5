WITH first_group AS(
    SELECT hk_group_id
      FROM IVIVCHIKYANDEXRU__DWH.h_groups
  ORDER BY registration_dt
     LIMIT 10
),
user_group_messages AS(
    SELECT lgd.hk_group_id,
           lum.hk_user_id,
           count(lum.hk_message_id) cnt_message
      FROM ivivchikyandexru__DWH.l_user_message lum
 LEFT JOIN ivivchikyandexru__DWH.l_groups_dialogs lgd
        ON lum.hk_message_id = lgd.hk_message_id
     WHERE hk_group_id IN (SELECT hk_group_id FROM first_group)
  GROUP BY lgd.hk_group_id, lum.hk_user_id  
),
user_group_log AS(
    SELECT hk_group_id,
           COUNT(DISTINCT hk_user_id) cnt_added_users
      FROM ivivchikyandexru__DWH.l_user_group_activity luga
      JOIN ivivchikyandexru__DWH.s_auth_group_history sagh
        ON luga.hk_l_user_group_activity = sagh.hk_l_user_group_activity
     WHERE "event" = 'add'
       AND hk_group_id IN (SELECT hk_group_id FROM first_group)
  GROUP BY hk_group_id
),
users_in_group_with_messages AS(
    SELECT hk_group_id,
           count(DISTINCT hk_user_id) cnt_users_in_group_with_messages
      FROM user_group_messages
     WHERE cnt_message >= 1
  GROUP BY hk_group_id
),
res AS(
    SELECT user_group_log.hk_group_id,
           cnt_added_users,
           cnt_users_in_group_with_messages,
           ROUND(cnt_users_in_group_with_messages / cnt_added_users * 100, 2) group_conversion
      FROM user_group_log
      JOIN users_in_group_with_messages
        ON user_group_log.hk_group_id = users_in_group_with_messages.hk_group_id
)

  SELECT hk_group_id,
         cnt_added_users,
         cnt_users_in_group_with_messages,
         group_conversion
    FROM res 
ORDER BY group_conversion DESC

