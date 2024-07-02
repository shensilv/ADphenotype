# Instructions on how to extract the refined Atopic Dermatitis phenotype from the UK BioBank research analysis platform. 

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

## 4 - Load cohort and select fields

## 5 - Extract fields into Spark dataframe and filter

## 6 - Convert to Pandas dataframe and save as csv to UKB project
