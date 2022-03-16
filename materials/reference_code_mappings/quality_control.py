import pandas 

df = pandas.read_csv("reference_code_mappings.csv")

print(df[df.duplicated("doi",keep=False)].dropna(axis="index",subset=["doi"]))

assert df["reference_code"].dropna().is_unique
assert df["pmid"].dropna().is_unique
assert df["doi"].dropna().is_unique
