# Instructions on how to extract the refined Atopic Dermatitis phenotype from the UK BioBank research analysis platform (RAP) 

## 1 - Set up jupyterlab on user interface

You will need to use Spark Jupyterlab. Jupyterlab is a web-based interaction development environment, and Spark provides the interfact for programming clusters to work in a parallel environment. Spark Jupyterlab allows you to work in a parallelised environment. 

We can access phenotypic data using SQL, do complex filtering and derive phenotypes (new columns) and transform data. Here we will create a phenotype file using the UKB dataset. 

### Load Spark Jupyterlab 

On the DNAnexus homepage, go to 'tools', then click 'JupyterLab'. From here, select the following options on normal priority. 

**runtime:** 2 hours (plenty of time)    
**recommended instance:** mem1_ssd1_v2_x16    
**estimated cost:** ~£0.5 or ~£1.8    

Once the jupyterlab instance has been initialised, click the link to open a jupyterlab window and open a python3 notebook. 

## 2 - Load packages and dataset ID

### Import packages and initialise spark session
```
import pyspark
import dxpy
import dxdata
import pandas as pd
```
```
# Spark initialization (Done only once; do not rerun this cell unless you select Kernel -> Restart kernel).
sc = pyspark.SparkContext()
spark = pyspark.sql.SparkSession(sc)
```
### Detect dataset ID which is located in project root directory

```
# Automatically discover dispensed database name and dataset id
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

## 3 - Access dataset and load cohort

### Load dataset into Spark

```
dataset = dxdata.load_dataset(id=dispensed_dataset_id)
```

We can see the entities in UKB by running `dataset.entities`. UK BioBank has the following entity tables, which are all linked to one another. The main entity in UKB is participant, and this corresponds to most of the phenotype fields in UKB. Other entities include linked healthcare records etc. To see the UKB entities, go to [this file](UKB_entities.txt). 

The entities we are interested in are: "participant" and "gp_clinical". Access these by running `participant = dataset["participant"]` and 

## 4 - Load cohort and select fields

We want to load each instance of self-report data, ICD9/10 and GP codes. For entity 'participant', we load self-report and ICD9/10. These codes are in the file [icdself_fieldnames.txt](icdself_fieldnames.txt). 

```
with open('icdself_fieldnames.txt', 'r') as file:
    # Read all lines, strip newline characters, and store them in a list
    fields = [line.strip() for line in file.readlines()]

# Print the list of lines
print(fields)
```

## 5 - Extract fields into Spark dataframe and filter
Remember to replace coding values when you extract fields! 
```
df = participant.retrieve_fields(names=fields, engine=dxdata.connect(), coding_values="replace")
```
Show as Spark dataframe: `df.show(5, truncate=False)`   
Show as Pandas dataframe: `df.limit(5).toPandas()`   

Now we filter for Atopic dermatitis cases. We will use the following filters: 

| Field name | Field ID | Filters|
|----------|----------|----------|
| ICD9 | 41271 | 691, 6918, 69180|
| ICD10 | 41270 | L20, L208, L209|
| Self report | 20002| eczema/dermatitis|

Create filter term for self-report (as there are 4 x 34 instances), then copy and paste into the `df.filter()` function. 

```
# Generate the list of conditions for self-report fields
conditions = [f'(df.p20002_i{i}_a{j} = "eczema/dermatitis")' for i in range(4) for j in range(34)]

# Join the conditions with ' AND ' to create the final string
conditions_string = ' | '.join(conditions)

print(conditions_string)
```
Then use `df.filter(<string>).count()` to count instances. To get the ICD9 and 10 cases, use:

```
icd10 = df.filter(array_contains(df.p41270,"L20 Atopic dermatitis") | array_contains(df.p41270,"L20.8 Other atopic dermatitis") | array_contains(df.p41270,"L20.9 Atopic dermatitis, unspecified"))

icd9 = df.filter(array_contains(df.p41271,"691 Atopic dermatitis and related conditions") | array_contains(df.p41271,"6918 Other atopic dermatitis and related conditions") | array_contains(df.p41271,"69180 Atopic dermatitis"))
```

The numbers are given below:

|Field name| Number of cases|
|----------|----------|
| Self report| 16045|
| ICD10 | 281|
|ICD9 | 23 | 

## 6 - Convert to Pandas dataframe and save as csv to UKB project

```
icd10_pd = icd10.toPandas()
icd10_pd.head()
icd10_pd.to_csv('icd10.csv', index=False)
```
Do this for all three sets of data (ICD9, ICD10, self report)
```
%%bash
dx upload icd10.csv --dest /
```
