# Here are instructions on how to extract the refined Atopic Dermatitis phenotype from the UK BioBank research analysis platform. 

## 1 - Set up jupyterlab on user interface

You will need to use Spark Jupyterlab. Jupyterlab is a web-based interaction development environment, and Spark provides the interfact for programming clusters to work in a parallel environment. Spark Jupyterlab allows you to work in a parallelised environment. 

We can access phenotypic data using SQL, do complex filtering and derive phenotypes (new columns) and transform data. Here we will create a phenotype file using the UKB dataset. 

### Load Spark Jupyterlab 

On the DNAnexus homepage, go to 'tools', then click 'JupyterLab'. From here, select the following options on normal priority. 

**runtime:** 2 hours (plenty of time)    
**recommended instance:** mem1_ssd1_v2_x16    
**estimated cost:** ~£0.5 or ~£1.8    

Once the jupyterlab instance has been initialised 

## 2 - Load packages and dataset ID

## 3 - Access dataset and load cohort

## 4 - Load cohort and select fields

## 5 - Extract fields into Spark dataframe and filter

## 6 - Convert to Pandas dataframe and save as csv to UKB project
