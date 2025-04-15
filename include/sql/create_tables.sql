CREATE OR REPLACE TABLE MEETUP_DB.TRANSFORM_DATA.ACTIVE_MEMBERS
AS
SELECT M."member_id", M."member_name", M."city", M."country", M."state", M."member_status", C."category_name"
FROM MEETUP_DB.RAW_DATA.MEMBERS AS M
LEFT JOIN MEETUP_DB.RAW_DATA.GROUPS AS G ON M."group_id" = G."group_id"
LEFT JOIN MEETUP_DB.RAW_DATA.CATEGORIES AS C ON G."category_id" = C."category_id"
WHERE M."member_status" = 'active';