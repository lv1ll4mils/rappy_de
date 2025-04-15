CREATE OR REPLACE TABLE MEETUP_DB.TRANSFORM_DATA.ACTIVE_MEMBERS
AS
SELECT M."member_id", M."member_name", M."city", M."country", M."state", M."member_status", C."category_name"
FROM MEETUP_DB.RAW_DATA.MEMBERS AS M
LEFT JOIN MEETUP_DB.RAW_DATA.GROUPS AS G ON M."group_id" = G."group_id"
LEFT JOIN MEETUP_DB.RAW_DATA.CATEGORIES AS C ON G."category_id" = C."category_id"
WHERE M."member_status" = 'active';

CREATE OR REPLACE TABLE MEETUP_DB.TRANSFORM_DATA.EVENT_TRACKING
AS
SELECT E."group.created", E."event_id", E."description", E."duration", E."fee.currency", E."fee.description", E."venue.city", E."venue.country", G."category.name", G."city", G."country", G."lat", G."lon",G."state", LISTAGG(TT."topic_name", ' | ') AS topic_names
FROM MEETUP_DB.RAW_DATA.EVENTS AS E
LEFT JOIN MEETUP_DB.RAW_DATA.GROUPS AS G ON E."group_id" = G."group_id"
LEFT JOIN MEETUP_DB.RAW_DATA.MEMBERS_TOPICS AS MT ON G."organizer.member_id" = MT."member_id"
LEFT JOIN MEETUP_DB.RAW_DATA.TOPICS AS TT ON MT."topic_id" = TT."topic_id"
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14;