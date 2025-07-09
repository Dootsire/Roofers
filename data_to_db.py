import sqlite3
import pandas
import clean

def check_table(cur, name):
    cur.execute('''SELECT name FROM sqlite_schema WHERE name=?;''', [name])
    return cur.fetchone()

con = sqlite3.connect("roofing.db")
cur = con.cursor()

if (not check_table(cur, "inspections")):
    df = clean.get_inspections()
    df.to_sql("inspections", con, if_exists='replace', index=False)
if (not check_table(cur, "licenses")):
    df = clean.get_licenses()
    df.to_sql("licenses", con, if_exists='replace', index=False)
if (not check_table(cur, "violations")):
    df = pandas.read_csv("data/osha_violation12.csv")
    df.to_sql("violations", con, if_exists='replace', index=False)
    df = pandas.read_csv("data/osha_violation13.csv")
    df.to_sql("violations", con, if_exists='append', index=False)

x = 0
output = ""
for entry in cur.execute('''SELECT DISTINCT estab_name
            FROM inspections
            LEFT OUTER JOIN licenses
            ON (inspections.estab_name = licenses.'Business Name')
            WHERE licenses.'Business Name' IS NULL
            and inspections.site_address != inspections.mail_street'''):
    output += entry[0] + '\n'
    x += 1
print("Establishment name not in business name: " + str(x))

with open("roofers_not_in_db.txt", 'w') as file:
    output = output.replace('&', ' & ')
    file.write(output)

x = 0
output = ""
for entry in cur.execute('''SELECT DISTINCT dba_name
            FROM inspections
            INNER JOIN licenses
            ON (inspections.dba_name = licenses.'Business Name')
            WHERE inspections.'dba_name' IS NOT NULL'''):
    output += entry[0] + '\n'
    x += 1

for entry in cur.execute('''SELECT DISTINCT dba_name
            FROM inspections
            INNER JOIN licenses
            ON (inspections.dba_name = licenses.'BusinessDBA')
            WHERE inspections.'dba_name' IS NOT NULL'''):
    output += entry[0] + '\n'
    x += 1

for entry in cur.execute('''SELECT DISTINCT estab_name
            FROM inspections
            INNER JOIN licenses
            ON (inspections.estab_name = licenses.'BusinessDBA')
            WHERE inspections.'estab_name' IS NOT NULL'''):
    output += entry[0] + '\n'
    x += 1

print("Roofers matching other parts of database: " + str(x))

with open("roofers_still_in_db.txt", 'w') as file:
    output = output.replace('&', ' & ')
    file.write(output)

with open("roofers_not_in_db.txt") as f1:
    set1 = set(f1.readlines())
with open("roofers_still_in_db.txt") as f2:
    set2 = set(f2.readlines())

nondups = set1 - set2
print("Roofers with no matching licenses: " + str(len(nondups)))

with open("no_matching_license.txt", "w") as out:
    out.writelines(nondups)

x = 0
for entry in cur.execute('''SELECT DISTINCT estab_name
                         FROM inspections'''):
    x += 1
print("Establishments inspected: " + str(x))

cur.close()