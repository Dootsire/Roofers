import pandas
import re

def filter_inspections(df) -> pandas.DataFrame:
    columns = ["safety_manuf","safety_const","safety_marit","health_manuf",
               "health_const","health_marit","migrant","sic_code","host_est_key"]
    df = df.drop(columns, axis=1)
    df = df[df["naics_code"] == 238160]
    df = df[df["site_state"] == "IL"]
    df = df[df["open_date"] > "2020"]
    return df

def clean_inspections(df) -> pandas.DataFrame:
    df = normalize_punctuation(df, "estab_name")

    business_endings = re.compile(r'\sLLC\b|\sCO\b|\sINC\b|\sCORP\b|\sPLLC\b|\sLTD\b')
    df["estab_name"] = df["estab_name"].str.replace(business_endings, '', regex=True)

    df[['estab_name', 'dba_name']] = df['estab_name'].str.split(' DBA ', n=1, expand=True)
    df['dba_name'] = df['dba_name'].str.split(' DBA ', n=1).str[1].fillna(df["dba_name"])

    df = df[~df["estab_name"].str.contains("UNKNOWN")]
    df = df[~df["estab_name"].str.contains("UNKNOW")]
    df = df[~df["estab_name"].str.contains("UNKOWN")]
    return df

def clean_licenses(df) -> pandas.DataFrame:
    df = normalize_punctuation(df, "Business Name")
    df = normalize_punctuation(df, "BusinessDBA")

    business_endings = re.compile(r'\sLLC\b|\sCO\b|\sINC\b|\sCORP\b|\sPLLC\b|\sLTD\b')
    df["Business Name"] = df["Business Name"].str.replace(business_endings, '', regex=True)
    df["BusinessDBA"] = df["BusinessDBA"].str.replace(business_endings, '', regex=True)

    df[['Business Name', 'BusinessDBA']] = df['Business Name'].str.split('DBA', n=1, expand=True)
    df["Business Name"] = df["Business Name"].str.strip()
    df["BusinessDBA"] = df["BusinessDBA"].str.strip()
    return df

def normalize_punctuation(df, column) -> pandas.DataFrame:
    df[column] = df[column].str.replace(r'.', '')
    df[column] = df[column].str.replace(r',', '')
    df[column] = df[column].str.replace(r':', '')
    df[column] = df[column].str.replace(r'(', '')
    df[column] = df[column].str.replace(r')', '')
    df[column] = df[column].str.replace(r' & ', '&')
    return df


def get_inspections() -> pandas.DataFrame:
    df = pandas.read_csv("data/osha_inspection5.csv")
    df = filter_inspections(df)
    df = clean_inspections(df)

    df2 = pandas.read_csv("data/osha_inspection4.csv")
    df2 = filter_inspections(df2)
    df2 = clean_inspections(df2)
    inspections = pandas.concat([df, df2], ignore_index=True)

    print("Inspections filtered and cleaned, with " + str(len(inspections)) + " rows")
    return inspections

def get_licenses() -> pandas.DataFrame:
    licenses = pandas.read_csv("data/idfpr_roofing_licenses.csv")
    licenses = clean_licenses(licenses)

    print("Licenses cleaned, with " + str(len(licenses)) + " rows")
    return licenses