import sqlite3
import csv

get_violations = '''SELECT violations.*
            FROM violations
            INNER JOIN inspections ON inspections.activity_nr = violations.activity_nr
            WHERE violations.delete_flag IS NULL'''

violations_by_company = '''
SELECT 
    inspections.estab_name,
    COUNT(violations.viol_type) AS violation_count,
    SUM(violations.current_penalty) AS total_fees
FROM 
    inspections
INNER JOIN 
    violations ON inspections.activity_nr = violations.activity_nr
WHERE
    violations.delete_flag IS NULL
GROUP BY
    inspections.estab_name
ORDER BY
    total_fees DESC
'''

violations_by_standard = '''
SELECT
    viol_cat,
    COUNT(*) AS total_viols,
    SUM(current_penalty) AS total_fees
FROM (
    SELECT
        SUBSTR(v.standard, 1, INSTR(v.standard || ' ', ' ') - 1) AS viol_cat,
        v.current_penalty
    FROM
        violations v
    INNER JOIN
        inspections i ON v.activity_nr = i.activity_nr
    WHERE
        v.delete_flag IS NULL
)
GROUP BY
    viol_cat
ORDER BY
    total_viols DESC;
'''

violations_by_type = '''
SELECT
    viol_type,
    COUNT(*) AS total_viols,
    SUM(current_penalty) AS total_fees
FROM (
    SELECT
        v.viol_type,
        v.current_penalty
    FROM
        violations v
    INNER JOIN
        inspections i ON v.activity_nr = i.activity_nr
    WHERE
        v.delete_flag IS NULL
)
GROUP BY
    viol_type
ORDER BY
    total_viols DESC;
'''

median_violation_by_type = '''
WITH ranked_penalties AS (
    SELECT 
        v.viol_type,
        v.current_penalty,
        COUNT(*) OVER (PARTITION BY v.viol_type) AS type_count,
        ROW_NUMBER() OVER (PARTITION BY v.viol_type ORDER BY v.current_penalty) AS row_num
    FROM 
        violations v
    INNER JOIN 
        inspections i ON v.activity_nr = i.activity_nr
    WHERE 
        v.delete_flag IS NULL
)
SELECT 
    viol_type,
    AVG(current_penalty) AS median_penalty
FROM 
    ranked_penalties
WHERE 
    row_num BETWEEN (type_count + 1) / 2 AND (type_count + 2) / 2
GROUP BY 
    viol_type;
'''

no_matching_zip_code = '''
WITH first_query AS (
    SELECT CAST(mail_zip as INT) as zip, COUNT(DISTINCT estab_name) as no_matching_license
    FROM inspections
    LEFT OUTER JOIN licenses
    ON (inspections.estab_name = licenses.'Business Name')
    WHERE licenses.'Business Name' IS NULL
    and inspections.site_address != inspections.mail_street
    and inspections.mail_state = 'IL'
    GROUP BY mail_zip),
second_query AS (
    SELECT CAST(mail_zip as INT) as zip, COUNT(DISTINCT estab_name) as total
    FROM inspections
    WHERE inspections.mail_state = 'IL'
    GROUP BY mail_zip
    ORDER BY total DESC)
SELECT first_query.zip, first_query.no_matching_license, second_query.total
FROM second_query
FULL OUTER JOIN first_query
ON first_query.zip = second_query.zip
WHERE first_query.zip != ''
ORDER BY first_query.no_matching_license DESC
'''

inspection_zip_codes = '''
SELECT CAST(site_zip as INT) as zip, COUNT(site_zip) as total
FROM inspections
GROUP BY zip
ORDER BY total
'''

total_inspections = '''
SELECT COUNT(activity_nr)
FROM inspections
'''

def print_top_10(query):
    cur.execute(query)
    i = 0
    y = 0
    z = 0
    for x in cur:
        print(x)
        y += x[1]
        z += x[2]

con = sqlite3.connect("roofing.db")
cur = con.cursor()

'''print_top_10(violations_by_company)
print('-----')
print_top_10(violations_by_standard)
print('-----')
print_top_10(violations_by_type)
print('-----')
print_top_10(median_violation_by_type)'''
print_top_10(no_matching_zip_code)
print('-----')
for x in cur.execute(total_inspections):
    print(x)

cur.execute(inspection_zip_codes)
rows = cur.fetchall()
with open('zip.csv', 'w', newline='') as csvfile:
    csv_writer = csv.writer(csvfile)
    csv_writer.writerow([description[0] for description in cur.description])
    csv_writer.writerows(rows)