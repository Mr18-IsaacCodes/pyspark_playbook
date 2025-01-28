drop table mydb.cricket_match;
CREATE TABLE mydb.cricket_match (
    match_id INT,
    team1 VARCHAR(20),
    team2 VARCHAR(20),
    result VARCHAR(20)
);

INSERT INTO mydb.cricket_match values(1,'ENG','NZ','NZ');
INSERT INTO mydb.cricket_match values(2,'PAK','NED','PAK');
INSERT INTO mydb.cricket_match values(3,'AFG','BAN','BAN');
INSERT INTO mydb.cricket_match values(4,'SA','SL','SA');
INSERT INTO mydb.cricket_match values(5,'AUS','IND','AUS');
INSERT INTO mydb.cricket_match values(6,'NZ','NED','NZ');
INSERT INTO mydb.cricket_match values(7,'ENG','BAN','ENG');
INSERT INTO mydb.cricket_match values(8,'SL','PAK','PAK');
INSERT INTO mydb.cricket_match values(9,'AFG','IND','IND');
INSERT INTO mydb.cricket_match values(10,'SA','AUS','SA');
INSERT INTO mydb.cricket_match values(11,'BAN','NZ','BAN');
INSERT INTO mydb.cricket_match values(12,'PAK','IND','IND');
INSERT INTO mydb.cricket_match values(13,'SA','IND','DRAW');


SELECT 
    teamName,
    COUNT(*) AS No_of_matches_palyed,
    SUM(win_flag) AS matchs_won,
    COUNT(*) - SUM(win_flag) AS matchs_lost
FROM
    (SELECT 
        team1 AS TeamName,
            CASE
                WHEN team1 = result THEN 1
                ELSE 0
            END AS win_flag
    FROM
        mydb.cricket_match UNION ALL SELECT 
        team2 AS TeamName,
            CASE
                WHEN team2 = result THEN 1
                ELSE 0
            END AS win_flag
    FROM
        mydb.cricket_match) c
GROUP BY teamName;
