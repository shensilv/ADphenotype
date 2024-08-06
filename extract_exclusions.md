# Extract EIDs exclusions

## 1) Run JupyterLab spark cluster

Initialise a JupyterLab spark cluster, see [extract_AD_RAP.md](extract_AD_RAP.md) for more information. 

## 2) Extract sex mismatch
```
import pyspark
import dxpy
import dxdata
import pandas as pd

sc = pyspark.SparkContext()
spark = pyspark.sql.SparkSession(sc)

dispensed_database = dxpy.find_one_data_object(
    classname="database", 
    name="app*", 
    folder="/", 
    name_mode="glob", 
    describe=True)
dispensed_database_name = dispensed_database["describe"]["name"]

dispensed_dataset = dxpy.find_one_data_object(
    typename="Dataset", 
    name="app*.dataset", 
    folder="/", 
    name_mode="glob")
dispensed_dataset_id = dispensed_dataset["id"]

dispensed_dataset_id
```

```
dataset = dxdata.load_dataset(id=dispensed_dataset_id)
participant = dataset["participant"]
fields = ['eid', 'p22001', 'p31', 'p22019', 'p22027', 'p22006']
df = participant.retrieve_fields(names=fields, engine=dxdata.connect(), coding_values="replace")
df.limit(5).toPandas()
sex_mismatch = df.filter(df.p22001 != df.p31)
sex_mismatch_pd = sex_mismatch.toPandas()
```

## 3) Extract individuals with potential aneuploidy

```
aneuploidy = df.filter(df.p22019 == 'Yes')
aneuploidy_pd = aneuploidy.toPandas()
```

## 4) Extract individuals who are outliers in terms of heterozygosity and missingness

```
outliers = df.filter(df.p22027 == 'Yes')
outliers_pd = outliers.toPandas()
```

## 5) Extract ancestry outliers

```
from pyspark.sql import functions as F
non_white = df.filter(df.p22006.isNull())
non_white.count()
```

Merging dataframes and then save as csv. Remember to upload to project. 

