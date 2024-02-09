import pandas

READ_CSV = False



if READ_CSV:
    df = pandas.read_csv("openTECR recuration - actual data.csv")
else:
    df = pandas.read_excel("openTECR recuration.ods", sheet_name="actual data")
df = df.replace({"col l/r": {"l":1,"r":2}})

## containing NaNs
print("containing NaNs:")
df_isna = df.isna()
subset=["part","page","col l/r","table from top", "entry nr"]
counter = 0
for row in df_isna.index:
    print_this = False
    for col in subset:
        if df_isna.loc[row,col]==True:
            print_this = True
    if print_this:
        #print(df.loc[row])
        counter += 1
print(f"A total of {counter} rows contained NaNs.")

## QC
if True:
    ## duplicates and errors
    test_df = df
    MANUALLY_EXCLUDED_DUPLICATES = [
        "54STA",
        "71TAN/JOH",
        "91HOR/UEH",
        "76SCH/KRI",
    ]
    test_df = test_df[~test_df.reference.isin(MANUALLY_EXCLUDED_DUPLICATES)]
    assert sum(test_df[((test_df["entry nr"]=="duplicate") | (test_df["entry nr"]=="error"))].id.isna())==0, ("Duplicate or error found for an empty-ID row", test_df[(((test_df["entry nr"]=="duplicate") | (test_df["entry nr"]=="error"))) & test_df.id.isna()])

#print("I am removing the following duplicates and errors:")
df = df[~((df["entry nr"]=="duplicate") | (df["entry nr"]=="error"))]

##QC
if True:
    ##check completeness of position annotation
    na_counter = df[["part","page","col l/r","table from top", "entry nr"]].isna().sum(axis="columns")
    assert len(df[~na_counter.isin([0,5])])==0, print(df[~na_counter.isin([0,5])][["id","reference","part","page","col l/r","table from top", "entry nr"]].to_string())

    assert len(df[df.reference.str.contains(" ").fillna(False)])==0, print(df[df.reference.str.contains(" ").fillna(False)].to_string())

## drop NaNs -- these entries just haven't been worked on
df = df.dropna(subset=["part","page","col l/r","table from top", "entry nr"])

## convert to ints
df[["part","page","col l/r","table from top", "entry nr"]] = df[["part","page","col l/r","table from top", "entry nr"]].astype(int)


## quality check new data
if True:
    ## id is unique
    assert len(df.dropna(subset=["id"]).id.unique()) == len(df.dropna(subset=["id"])), df.dropna(subset=["id"])[df.dropna(subset=["id"]).id.duplicated()].to_string()

    ## tables intact in themselves
    for which, g in df.groupby(["part","page","col l/r","table from top"]):
        #print((which,g))
        assert len(g.reference.unique())==1, (which, print(g.to_string()))
        assert len(g.EC.unique()) == 1, (which, print(g.to_string()))
        assert len(g.description.unique())==1, (which, print(g.to_string()))
        assert sorted(g["entry nr"].values)==list(range(1,g["entry nr"].max()+1)), (which, print(g.to_string()))

    ## table counts consistently continuous
    for which, g in df.groupby(["part", "page", "col l/r"]):
        ## the following part/page/column combinations
        #
        # contain a table which was mentioned before;
        ## the table is thus marked full duplicate and was removed beforehand; so we manually
        ## exclude those from this automated check
        MANUALLY_EXCLUDED_COLUMNS = [
            (2, 558, 2),
            (2, 560, 2),
            (2, 566, 2),
            (2, 584, 1),
            (2, 590, 2),
            (3, 1041, 1),
            (3, 1076, 2),

        ]
        if which in MANUALLY_EXCLUDED_COLUMNS:
            continue
        assert sorted(g["table from top"].unique()) == list(range(1, g["table from top"].max() + 1)), (which, print(g.to_string()))

    ## column values either 1 or 2
    for which, g in df.groupby(["part", "page"]):
        assert all([i in [1,2] for i in g["col l/r"].values]), (which, print(g.to_string()))

    ## page numbers consistently continuous
    for which, g in df.groupby("part"):
        MANUALLY_EXCLUDED_PARTS = [
            #2,
            #3,
            #7
        ]
        if which in MANUALLY_EXCLUDED_PARTS:
            continue

        print(f"----- This is about part {which} -----")
        should_be = list(range(g["page"].min(), g["page"].max() + 1))
        for page in should_be:
            if page not in g["page"].unique():
                print(f"Missing page: {page}")
        #assert sorted(g["page"].unique()) == list(range(g["page"].min(), g["page"].max() + 1))

## extract added values
# print(f"You will need to care for these {df.id.isna().sum()} recently added rows:")
# print(df[df.id.isna()])

print("The online spreadsheet data looks consistent.")



## consistency check between online spreadsheet and original Noor data
noor = pandas.read_csv("TECRDB.csv")
if READ_CSV:
    online = pandas.read_csv("openTECR recuration - actual data.csv")
else:
    online = pandas.read_excel("openTECR recuration.ods", sheet_name="actual data")
online = online.replace({"col l/r": {"l":1,"r":2}})
## check that all ids are still there
assert set(noor.id) - set(online.id) == set(), f"The following IDs were deleted online: {set(noor.id)-set(online.id)}"
## non-curated values should not have been changed by anyone!
leftjoined = pandas.merge(noor, online.dropna(subset="id"), on="id", how="left", validate="1:1")
SHOULD_BE_THE_SAME = [
    "reference",
    "EC",
    "description",
    "K",
    "temperature",
    "ionic_strength",
    "p_h",
    "p_mg",
    #"K_prime",
]
for s in SHOULD_BE_THE_SAME:
    entries_where_both_are_nans = leftjoined[ leftjoined[f"{s}_x"].isna() & leftjoined[f"{s}_y"].isna() ]
    if len(entries_where_both_are_nans) == 0:
        assert (leftjoined[f"{s}_x"] == leftjoined[f"{s}_y"]).all(), (s, print(leftjoined[~(leftjoined[f"{s}_x"] == leftjoined[f"{s}_y"])][["id",f"{s}_x",f"{s}_y"]].to_string()))
    else:
        tmp = leftjoined[ ~ (leftjoined[f"{s}_x"].isna() & leftjoined[f"{s}_y"].isna()) ]
        assert (tmp[f"{s}_x"] == tmp[f"{s}_y"]).all(), (s, print(tmp[~(tmp[f"{s}_x"] == tmp[f"{s}_y"])][["id",f"{s}_x",f"{s}_y"]].to_string()))

## did someone add a new row without id, where they should have corrected a row with id?
merged = pandas.merge(online, noor, on=[    "reference",
    "temperature",
    "ionic_strength",
    "p_h",
    "p_mg",
    "K_prime",
])
merged = merged[merged["id_x"] != merged["id_y"]]
potential_errors = merged[merged["id_x"].isna() | merged["id_y"].isna()]
## the following have been manually checked to be able to be excluded from the comparison below:
MANUALLY_EXCLUDED = [
    "https://w3id.org/related-to/doi.org/10.5281/zenodo.3978439/files/TECRDB.csv#entry4356",
    "https://w3id.org/related-to/doi.org/10.5281/zenodo.3978439/files/TECRDB.csv#entry1714",
    "https://w3id.org/related-to/doi.org/10.5281/zenodo.3978439/files/TECRDB.csv#entry1715",
    "https://w3id.org/related-to/doi.org/10.5281/zenodo.3978439/files/TECRDB.csv#entry1716",
    "https://w3id.org/related-to/doi.org/10.5281/zenodo.3978439/files/TECRDB.csv#entry1718",
    "https://w3id.org/related-to/doi.org/10.5281/zenodo.3978439/files/TECRDB.csv#entry1717",
    "https://w3id.org/related-to/doi.org/10.5281/zenodo.3978439/files/TECRDB.csv#entry2140",
    "https://w3id.org/related-to/doi.org/10.5281/zenodo.3978439/files/TECRDB.csv#entry2149",
    "https://w3id.org/related-to/doi.org/10.5281/zenodo.3978439/files/TECRDB.csv#entry3167",
    "https://w3id.org/related-to/doi.org/10.5281/zenodo.3978439/files/TECRDB.csv#entry3184",
    "https://w3id.org/related-to/doi.org/10.5281/zenodo.3978439/files/TECRDB.csv#entry707",
    "https://w3id.org/related-to/doi.org/10.5281/zenodo.3978439/files/TECRDB.csv#entry4246",
    "https://w3id.org/related-to/doi.org/10.5281/zenodo.3978439/files/TECRDB.csv#entry392",
    "https://w3id.org/related-to/doi.org/10.5281/zenodo.3978439/files/TECRDB.csv#entry1269",
    "https://w3id.org/related-to/doi.org/10.5281/zenodo.3978439/files/TECRDB.csv#entry4283",
    "https://w3id.org/related-to/doi.org/10.5281/zenodo.3978439/files/TECRDB.csv#entry4284",
    "https://w3id.org/related-to/doi.org/10.5281/zenodo.3978439/files/TECRDB.csv#entry3888",
    "https://w3id.org/related-to/doi.org/10.5281/zenodo.3978439/files/TECRDB.csv#entry3896",
    "https://w3id.org/related-to/doi.org/10.5281/zenodo.3978439/files/TECRDB.csv#entry3897",
    "https://w3id.org/related-to/doi.org/10.5281/zenodo.3978439/files/TECRDB.csv#entry1915",
    "https://w3id.org/related-to/doi.org/10.5281/zenodo.3978439/files/TECRDB.csv#entry1603",
    "https://w3id.org/related-to/doi.org/10.5281/zenodo.3978439/files/TECRDB.csv#entry1605",
    "https://w3id.org/related-to/doi.org/10.5281/zenodo.3978439/files/TECRDB.csv#entry236",
    "https://w3id.org/related-to/doi.org/10.5281/zenodo.3978439/files/TECRDB.csv#entry386",
    "https://w3id.org/related-to/doi.org/10.5281/zenodo.3978439/files/TECRDB.csv#entry2718",
    "https://w3id.org/related-to/doi.org/10.5281/zenodo.3978439/files/TECRDB.csv#entry3560",
    "https://w3id.org/related-to/doi.org/10.5281/zenodo.3978439/files/TECRDB.csv#entry3561",
    "https://w3id.org/related-to/doi.org/10.5281/zenodo.3978439/files/TECRDB.csv#entry4006",
    "https://w3id.org/related-to/doi.org/10.5281/zenodo.3978439/files/TECRDB.csv#entry1271",
    "https://w3id.org/related-to/doi.org/10.5281/zenodo.3978439/files/TECRDB.csv#entry1586",
    "https://w3id.org/related-to/doi.org/10.5281/zenodo.3978439/files/TECRDB.csv#entry1589",
    "https://w3id.org/related-to/doi.org/10.5281/zenodo.3978439/files/TECRDB.csv#entry1590",
    "https://w3id.org/related-to/doi.org/10.5281/zenodo.3978439/files/TECRDB.csv#entry1591",
    "https://w3id.org/related-to/doi.org/10.5281/zenodo.3978439/files/TECRDB.csv#entry1588",
    "https://w3id.org/related-to/doi.org/10.5281/zenodo.3978439/files/TECRDB.csv#entry1587",
    "https://w3id.org/related-to/doi.org/10.5281/zenodo.3978439/files/TECRDB.csv#entry2370",
    "https://w3id.org/related-to/doi.org/10.5281/zenodo.3978439/files/TECRDB.csv#entry2371",
    "https://w3id.org/related-to/doi.org/10.5281/zenodo.3978439/files/TECRDB.csv#entry1601",
    "https://w3id.org/related-to/doi.org/10.5281/zenodo.3978439/files/TECRDB.csv#entry3637",
    "https://w3id.org/related-to/doi.org/10.5281/zenodo.3978439/files/TECRDB.csv#entry2725",
    "https://w3id.org/related-to/doi.org/10.5281/zenodo.3978439/files/TECRDB.csv#entry705",
    "https://w3id.org/related-to/doi.org/10.5281/zenodo.3978439/files/TECRDB.csv#entry702",
    "https://w3id.org/related-to/doi.org/10.5281/zenodo.3978439/files/TECRDB.csv#entry704",
    "https://w3id.org/related-to/doi.org/10.5281/zenodo.3978439/files/TECRDB.csv#entry4040",
    "https://w3id.org/related-to/doi.org/10.5281/zenodo.3978439/files/TECRDB.csv#entry4373",
    "https://w3id.org/related-to/doi.org/10.5281/zenodo.3978439/files/TECRDB.csv#entry1829",
    "https://w3id.org/related-to/doi.org/10.5281/zenodo.3978439/files/TECRDB.csv#entry3031",
    "https://w3id.org/related-to/doi.org/10.5281/zenodo.3978439/files/TECRDB.csv#entry3032",
    "https://w3id.org/related-to/doi.org/10.5281/zenodo.3978439/files/TECRDB.csv#entry2339",
    "https://w3id.org/related-to/doi.org/10.5281/zenodo.3978439/files/TECRDB.csv#entry1916",
    "https://w3id.org/related-to/doi.org/10.5281/zenodo.3978439/files/TECRDB.csv#entry3026",
    "https://w3id.org/related-to/doi.org/10.5281/zenodo.3978439/files/TECRDB.csv#entry4086",

]
potential_errors = potential_errors[ ~potential_errors.id_y.isin(MANUALLY_EXCLUDED) ]
if len(potential_errors) > 0:
    print("The following entries might have been added as a new row without id, where they should have corrected a row with id:")
    print(potential_errors.to_string())

print("The online spreadsheet data and its original data source are still in sync.")



## annotate online spreadsheet with table_codes
#
#
## read second df
## -- get it manually from https://zenodo.org/record/5495826 !
noor = pandas.read_csv("TECRDB.csv")
noor.columns
#Index(['id', 'url', 'reference', 'method', 'eval', 'EC', 'enzyme_name',
#       'reaction', 'description', 'K', 'K_prime', 'temperature',
#       'ionic_strength', 'p_h', 'p_mg'],

noor = noor.drop(['K', 'K_prime', 'temperature',
       'ionic_strength', 'p_h', 'p_mg'],axis="columns")

## extract table codes
noor["table_code"] = noor.url.str.split("&T1=").str[-1]

## quality check
if True:
    ## tables intact in themselves
    for which, g in noor.groupby("table_code"):
        #print((which,g))
        assert len(g.reference.unique())==1, (which, print(g.to_string()))
        assert len(g.method.unique())==1, (which, print(g.to_string()))
        assert len(g["eval"].unique()) == 1, (which, print(g.to_string()))
        assert len(g.EC.unique()) == 1, (which, print(g.to_string()))
        assert len(g.enzyme_name.unique())==1, (which, print(g.to_string()))
        assert len(g.reaction.unique()) == 1, (which, print(g.to_string()))
        assert len(g.description.unique()) == 1, (which, print(g.to_string()))

## drop now-unnecessary columns
noor = noor.drop(["EC","reference", "description"], axis="columns")
#df   = df.drop(  ["description"],    axis="columns")

## merge
tmp = pandas.merge(df, noor, how="left", on="id")

## add manually extracted table codes
if READ_CSV:
    manual_table_codes = pandas.read_csv("openTECR recuration - table codes.csv")
else:
    manual_table_codes = pandas.read_excel("openTECR recuration.ods", sheet_name="manually mapped table codes")

# QC
if True:
    assert sum(manual_table_codes.duplicated(["part", "page", "col l/r", "table from top"])) == 0, print(
        manual_table_codes[manual_table_codes.duplicated(["part", "page", "col l/r", "table from top"])])
# split into tables with table codes from Noor and those which needed to be annotated manually
manual_table_codes = manual_table_codes.drop(["reference", "description"], axis="columns")
tmp_with_table_codes = tmp[~tmp.table_code.isna()]
tmp_without_table_codes = tmp[tmp.table_code.isna()]
tmp_without_table_codes = tmp_without_table_codes.drop("table_code", axis="columns")
tmp_without_table_codes_try_to_add_manual_ones = pandas.merge(tmp_without_table_codes, manual_table_codes, how="left", on=["part","page","col l/r","table from top"])
# concat the two
new = pandas.concat([tmp_with_table_codes, tmp_without_table_codes_try_to_add_manual_ones], ignore_index=True)
## keep only one entry per table code, remove now-meaningless columns, but keep id=NaN rows
new = new[~new.duplicated(["part","page","col l/r","table from top"])]
new = new.drop(["id","url"], axis="columns")
new["comment"] = ""

## export tables which need to be added to the "table codes" tab
selector = []
for i,s in new.iterrows():
    if pandas.isna(s.table_code):
        if len(manual_table_codes[(manual_table_codes.part == s.part) &
                            (manual_table_codes.page == s.page) &
                            (manual_table_codes["col l/r"] == s["col l/r"]) &
                            (manual_table_codes["table from top"] == s["table from top"])
           ]) == 0:
            selector.append(i)
export = new.loc[selector]
(export[[
    "part","page","col l/r","table from top",
    "table_code",
    "description",
    "reference",
    "curator",
    ]]
 .sort_values(["part","page","col l/r","table from top"])
 .to_csv("2024-01-06-opentecr-recuration.missing_table_codes.csv", index=False)
 )


## export tables which need to have their comment extracted
selector = []
if READ_CSV:
    tables_with_comments = pandas.read_csv("openTECR recuration - table metadata.csv")
else:
    tables_with_comments = pandas.read_excel("openTECR recuration.ods", sheet_name="table comments")
## QC
if True:
    assert sum(tables_with_comments.duplicated(["part","page","col l/r","table from top"]))==0, print(tables_with_comments[tables_with_comments.duplicated(["part","page","col l/r","table from top"])])
## extract only the tables which are not mentioned in the spreadsheet yet
for i, s in new.iterrows():
    if len(tables_with_comments[(tables_with_comments.part == s.part) &
                               (tables_with_comments.page == s.page) &
                               (tables_with_comments["col l/r"] == s["col l/r"]) &
                               (tables_with_comments["table from top"] == s["table from top"])
          ]) == 0:
       selector.append(i)
tables_without_comments = new.loc[selector]
tables_without_comments["manually spellchecked"] = ""
tables_without_comments["comment"] = ""

(tables_without_comments[[
    "part","page","col l/r","table from top",
    "reference",
    "manually spellchecked",
    "comment"
    ]]
 .sort_values(["part","page","col l/r","table from top"])
 .to_csv("2024-01-06-opentecr-recuration.missing-table-comments.csv", index=False)
 )


print("I could merge the online spreadsheet and the Noor data. I have written file regarding the tables to your disk.")
