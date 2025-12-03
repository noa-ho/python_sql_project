
IF DB_ID('PaymentFormulasDB') IS NULL
BEGIN
    CREATE DATABASE PaymentFormulasDB;
END
GO

USE PaymentFormulasDB;
GO

-- טבלת נתונים: data_t--
IF OBJECT_ID('data_t', 'U') IS NOT NULL
    DROP TABLE data_t;
GO

CREATE TABLE data_t (
    data_id INT IDENTITY(1,1) PRIMARY KEY,
    a FLOAT NOT NULL,
    b FLOAT NOT NULL,
    c FLOAT NOT NULL,
    d FLOAT NOT NULL
);
GO

--  טבלת נוסחאות: targil_t--
IF OBJECT_ID('targil_t', 'U') IS NOT NULL
    DROP TABLE targil_t;
GO

CREATE TABLE targil_t (
    targil_id INT IDENTITY(1,1) PRIMARY KEY,
    targil VARCHAR(255) NULL,
    tnai VARCHAR(255) NULL,
    false_targil VARCHAR(255) NULL
);
GO

--  טבלת תוצאות: results_t--
IF OBJECT_ID('results_t', 'U') IS NOT NULL
    DROP TABLE results_t;
GO

CREATE TABLE results_t (
    results_id INT IDENTITY(1,1) PRIMARY KEY,
    data_id INT NOT NULL FOREIGN KEY REFERENCES data_t(data_id),
    targil_id INT NOT NULL FOREIGN KEY REFERENCES targil_t(targil_id),
    method VARCHAR(50) NOT NULL,
    result FLOAT NOT NULL
);
GO

--  טבלת לוג: log_t--
IF OBJECT_ID('log_t', 'U') IS NOT NULL
    DROP TABLE log_t;
GO

CREATE TABLE log_t (
    log_id INT IDENTITY(1,1) PRIMARY KEY,
    targil_id INT NOT NULL FOREIGN KEY REFERENCES targil_t(targil_id),
    method VARCHAR(50) NOT NULL,
    time_run FLOAT NOT NULL
);
GO


-- אפשרות ליצירת מיליון רשומות בנתונים אקראיים--

WITH Numbers AS (
    SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
    UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8
    UNION ALL SELECT 9 UNION ALL SELECT 10
)
INSERT INTO data_t (a,b,c,d)
SELECT 
    RAND(CHECKSUM(NEWID())) * 100,
    RAND(CHECKSUM(NEWID())) * 100,
    RAND(CHECKSUM(NEWID())) * 100,
    RAND(CHECKSUM(NEWID())) * 100
FROM Numbers n1
CROSS JOIN Numbers n2
CROSS JOIN Numbers n3
CROSS JOIN Numbers n4
CROSS JOIN Numbers n5
CROSS JOIN Numbers n6; -- 10^6 = 1,000,000 רשומות
GO


--מילוי targil_t עם מספר נוסחאות לדוגמה--
INSERT INTO targil_t (targil, tnai, false_targil) VALUES
('2 * c', NULL, NULL),
('SQRT(POWER(2,d) + POWER(2,c))', NULL, NULL),
('LN(c) + b', NULL, NULL),
('1', 'a = c', '0'),
-- נוסחאות מורכבות
('ROUND(a * b / (c + 1), 2) + SQRT(d)', NULL, NULL),
('LN(a+1) + LN(b+1) + LN(c+1)', NULL, NULL),
-- נוסחאות עם תנאי
('a + 5', 'b < 20', 'd - 5'),
(
    'SQRT(ABS(b - c) + LN(d + 1)) + POWER(a, 1.5)',  
    'a > 50',                                       
    'EXP(a/100) + b - c'                            
);
GO


---פרוצדורה---
CREATE OR ALTER PROCEDURE sp_CalculateFormulas_Fast
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE 
        @sql NVARCHAR(MAX),
        @targil_id INT,
        @targil VARCHAR(MAX),
        @tnai VARCHAR(MAX),
        @false_targil VARCHAR(MAX),
        @startTime DATETIME,
        @endTime DATETIME;

    DECLARE cur CURSOR FOR
        SELECT targil_id, targil, tnai, false_targil
        FROM targil_t;

    OPEN cur;
    FETCH NEXT FROM cur INTO @targil_id, @targil, @tnai, @false_targil;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @startTime = GETDATE();

        IF @tnai IS NULL
        BEGIN
            SET @sql = N'
                INSERT INTO results_t (data_id, targil_id, method, result)
                SELECT 
                    data_id,
                    ' + CAST(@targil_id AS VARCHAR(10)) + ',
                    ''SQL-Fast'',
                    ' + @targil + '
                FROM data_t;
            ';
        END
        ELSE
        BEGIN
            SET @sql = N'
                INSERT INTO results_t (data_id, targil_id, method, result)
                SELECT 
                    data_id,
                    ' + CAST(@targil_id AS VARCHAR(10)) + ',
                    ''SQL-Fast'',
                    CASE 
                        WHEN ' + @tnai + ' 
                        THEN ' + @targil + '
                        ELSE ' + @false_targil + '
                    END
                FROM data_t;
            ';
        END

        -- טיפול בשגיאות מתמטיות
        BEGIN TRY
            EXEC sp_executesql @sql;
        END TRY
        BEGIN CATCH
            -- במקרה של שגיאה, הכנס 0 לכל הרשומות של הנוסחה הזו
            INSERT INTO results_t (data_id, targil_id, method, result)
            SELECT data_id, @targil_id, 'SQL-Fast', 0
            FROM data_t;

            -- רישום שגיאה בלוג
            INSERT INTO log_t (targil_id, method, time_run)
            VALUES (@targil_id, 'SQL-Fast-Error', 0);
        END CATCH;

        SET @endTime = GETDATE();

        INSERT INTO log_t (targil_id, method, time_run)
        VALUES (
            @targil_id,
            'SQL-Fast',
            DATEDIFF(MILLISECOND, @startTime, @endTime) * 1.0 / 1000
        );

        FETCH NEXT FROM cur INTO @targil_id, @targil, @tnai, @false_targil;
    END

    CLOSE cur;
    DEALLOCATE cur;
END
--השוואה--
SELECT sql.data_id, sql.targil_id,
       sql.result AS sql_result,
       py.result  AS py_result
FROM results_t sql
JOIN results_t py
    ON sql.data_id = py.data_id
    AND sql.targil_id = py.targil_id
WHERE sql.method = 'SQL-Fast'
  AND py.method = 'Python'
  AND ISNULL(sql.result, 0) <> ISNULL(py.result, 0);


--בדיקות--

SELECT * 
FROM data_t
WHERE data_id = 1000014;


SELECT targil_id, COUNT(*) AS RecordsPerTargil
FROM results_t
GROUP BY targil_id;

SELECT targil_id, COUNT(*) AS RecordsPerTargil
FROM results_t
GROUP BY targil_id
ORDER BY targil_id;

SELECT *
FROM results_t
ORDER BY results_id DESC;

SELECT *
FROM results_t
WHERE targil_id = 18

SELECT  TOP 10*
FROM log_t
WHERE method = 'Python_Pandas_Eval_Fixed';

SELECT *
FROM log_t
ORDER BY log_id DESC;


SELECT COUNT(*) AS TotalRecords FROM results_t;
SELECT TOP 200 * FROM results_t ORDER BY results_id DESC;
