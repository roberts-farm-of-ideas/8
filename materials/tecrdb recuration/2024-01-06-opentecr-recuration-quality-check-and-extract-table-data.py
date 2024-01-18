import pandas

df = pandas.read_csv("openTECR recuration - actual data.csv")
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

## duplicates and errors
#print("I am removing the following duplicates and errors:")
#print(df[(df["entry nr"]=="duplicate") | (df["entry nr"]=="error")])
df = df[~((df["entry nr"]=="duplicate") | (df["entry nr"]=="error"))]

## drop NaNs -- these entries just haven't been worked on
df = df.dropna(subset=["part","page","col l/r","table from top", "entry nr"])

## convert to ints
df[["part","page","col l/r","table from top", "entry nr"]] = df[["part","page","col l/r","table from top", "entry nr"]].astype(int)


## quality check new data
if True:
    ## tables intact in themselves
    for which, g in df.groupby(["part","page","col l/r","table from top"]):
        #print((which,g))
        assert len(g.reference.unique())==1
        assert len(g.EC.unique()) == 1
        assert len(g.description.unique())==1
        assert sorted(g["entry nr"].values)==list(range(1,g["entry nr"].max()+1))

    ## table counts consistently continuous
    for which, g in df.groupby(["part", "page", "col l/r"]):
        assert sorted(g["table from top"].unique()) == list(range(1, g["table from top"].max() + 1))

    ## column values either 1 or 2
    for which, g in df.groupby(["part", "page"]):
        assert all([i in [1,2] for i in g["col l/r"].values])

    ## page numbers consistently continuous
    for which, g in df.groupby("part"):
        assert sorted(g["page"].unique()) == list(range(g["page"].min(), g["page"].max() + 1))

## extract added values
# print(f"You will need to care for these {df.id.isna().sum()} recently added rows:")
# print(df[df.id.isna()])

print("The online spreadsheet data looks consistent.")



## consistency check between online spreadsheet and original Noor data
## -- non-curated values should not have been changed by anyone!
noor = pandas.read_csv("TECRDB.csv")
online = pandas.read_csv("openTECR recuration - actual data.csv")
online = online.replace({"col l/r": {"l":1,"r":2}})
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
        assert (leftjoined[f"{s}_x"] == leftjoined[f"{s}_y"]).all(), s #print(leftjoined[~(leftjoined[f"{s}_x"] == leftjoined[f"{s}_y"])])
    else:
        tmp = leftjoined[ ~ (leftjoined[f"{s}_x"].isna() & leftjoined[f"{s}_y"].isna()) ]
        assert (tmp[f"{s}_x"] == tmp[f"{s}_y"]).all(), s  # print(leftjoined[~(leftjoined[f"{s}_x"] == leftjoined[f"{s}_y"])])

## did someone add a new row without id, where they should have corrected a row with id?
merged = pandas.merge(online, noor, on=[    "reference",
    "EC",
    "description",
    "temperature",
    "ionic_strength",
    "p_h",
    "p_mg",
    "K_prime",
])
merged = merged[merged["id_x"] != merged["id_y"]]
potential_errors = merged[merged["id_x"].isna() | merged["id_y"].isna()]
MANUALLY_EXCLUDED = [
"https://w3id.org/related-to/doi.org/10.5281/zenodo.3978439/files/TECRDB.csv#entry4356",
]
potential_errors = potential_errors[ ~potential_errors.id_y.isin(MANUALLY_EXCLUDED) ]
if len(potential_errors) > 0:
    print("The following entries might have been added as a new row without id, where they should have corrected a row with id:")
    print(potential_errors)

print("The online spreadsheet data and its original data source are still in sync.")


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

## remove known duplicates
#REMOVE = []
#noor = noor[ ~noor.id.isin(REMOVE) ]

## quality check
if True:
    ## tables intact in themselves
    for which, g in noor.groupby("table_code"):
        #print((which,g))
        assert len(g.reference.unique())==1
        assert len(g.method.unique())==1
        assert len(g["eval"].unique()) == 1
        assert len(g.EC.unique()) == 1
        assert len(g.enzyme_name.unique())==1
        assert len(g.reaction.unique()) == 1
        assert len(g.description.unique()) == 1

## drop now-unnecessary columns
noor = noor.drop(["EC","reference"], axis="columns")
df   = df.drop(  ["description"],    axis="columns")

## drop rows without id
#df = df[~df.id.isna()]

## merge
new = pandas.merge(df, noor, how="left", on="id")

## keep only one entry per table code, remove now-meaningless columns, but keep id=NaN rows
#new1 = new[~new.duplicated("table_code")]
#new1 = new1.dropna(subset=["table_code"])
#new2 = new[new.table_code.isna()]
#new2 = new2[~new2.duplicated(["part","page","col l/r","table from top"])]
#new = pandas.concat([new1,new2])
new = new[~new.duplicated(["part","page","col l/r","table from top"])]
new = new.drop(["id","url"], axis="columns")

## write out
new["comment"] = ""
new["buffer"] = ""
(new[[
    "part","page","col l/r","table from top",
    "table_code",
    #"enzyme_name",
    #"EC",
    "description",
    "reference",
    #"method",
    #"buffer",
    #"eval",
    "comment"
    ]]
 .sort_values(["part","page","col l/r","table from top"])
 .to_csv("2024-01-06-opentecr-recuration-extract-data.py.out.table-list.csv", index=False)
 )

print("I could merge the online spreadsheet and the Noor data. I have written a file containing the table metadata to your disk.")
