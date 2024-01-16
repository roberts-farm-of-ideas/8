import pandas

df = pandas.read_csv("openTECR recuration - actual data.csv")

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

## drop NaNs
df = df.dropna(subset=["part","page","col l/r","table from top", "entry nr"])

## duplicates and errors
print("duplicates and errors:")
print(df[(df["entry nr"]=="duplicate") | (df["entry nr"]=="error")])
df = df[~((df["entry nr"]=="duplicate") | (df["entry nr"]=="error"))]

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
    for which, g in df.groupby(["part"]):
        assert sorted(g["page"].unique()) == list(range(g["page"].min(), g["page"].max() + 1))

## extract added values
# print(f"You will need to care for these {df.id.isna().sum()} recently added rows:")
# print(df[df.id.isna()])


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
REMOVE = [
       'https://w3id.org/related-to/doi.org/10.5281/zenodo.3978439/files/TECRDB.csv#entry895',
       'https://w3id.org/related-to/doi.org/10.5281/zenodo.3978439/files/TECRDB.csv#entry896',
       'https://w3id.org/related-to/doi.org/10.5281/zenodo.3978439/files/TECRDB.csv#entry897',
       'https://w3id.org/related-to/doi.org/10.5281/zenodo.3978439/files/TECRDB.csv#entry898',
       'https://w3id.org/related-to/doi.org/10.5281/zenodo.3978439/files/TECRDB.csv#entry899'
]
noor = noor[ ~noor.id.isin(REMOVE) ]

## quality check
if True:
    ## tables intact in themselves
    for which, g in noor.groupby(["table_code"]):
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
new = pandas.merge(df, noor, how="left", on="id")#, validate="1:1")

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
    "table_code",
    "part","page","col l/r","table from top",
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

print("I have written a file containing the tables.")
