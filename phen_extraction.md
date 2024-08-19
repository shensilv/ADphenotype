```python
import pyspark
import dxpy
import dxdata
import pandas as pd
```


```python
# Spark initialization (Done only once; do not rerun this cell unless you select Kernel -> Restart kernel).
sc = pyspark.SparkContext()
spark = pyspark.sql.SparkSession(sc)
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




    'record-Gfj4Z4jJ8qJ45pgxp5PkjYQq'




```python
dataset = dxdata.load_dataset(id=dispensed_dataset_id)
participant = dataset["participant"]
```


```python
with open('icdself_fieldnames.txt', 'r') as file:
    # Read all lines, strip newline characters, and store them in a list
    fields = [line.strip() for line in file.readlines()]

# Print the list of lines
print(fields)
```

    ['eid', 'p41271', 'p41270', 'p41202', 'p41203', 'p41204', 'p41205', 'p20002_i0_a0', 'p20002_i0_a1', 'p20002_i0_a2', 'p20002_i0_a3', 'p20002_i0_a4', 'p20002_i0_a5', 'p20002_i0_a6', 'p20002_i0_a7', 'p20002_i0_a8', 'p20002_i0_a9', 'p20002_i0_a10', 'p20002_i0_a11', 'p20002_i0_a12', 'p20002_i0_a13', 'p20002_i0_a14', 'p20002_i0_a15', 'p20002_i0_a16', 'p20002_i0_a17', 'p20002_i0_a18', 'p20002_i0_a19', 'p20002_i0_a20', 'p20002_i0_a21', 'p20002_i0_a22', 'p20002_i0_a23', 'p20002_i0_a24', 'p20002_i0_a25', 'p20002_i0_a26', 'p20002_i0_a27', 'p20002_i0_a28', 'p20002_i0_a29', 'p20002_i0_a30', 'p20002_i0_a31', 'p20002_i0_a32', 'p20002_i0_a33', 'p20002_i1_a0', 'p20002_i1_a1', 'p20002_i1_a2', 'p20002_i1_a3', 'p20002_i1_a4', 'p20002_i1_a5', 'p20002_i1_a6', 'p20002_i1_a7', 'p20002_i1_a8', 'p20002_i1_a9', 'p20002_i1_a10', 'p20002_i1_a11', 'p20002_i1_a12', 'p20002_i1_a13', 'p20002_i1_a14', 'p20002_i1_a15', 'p20002_i1_a16', 'p20002_i1_a17', 'p20002_i1_a18', 'p20002_i1_a19', 'p20002_i1_a20', 'p20002_i1_a21', 'p20002_i1_a22', 'p20002_i1_a23', 'p20002_i1_a24', 'p20002_i1_a25', 'p20002_i1_a26', 'p20002_i1_a27', 'p20002_i1_a28', 'p20002_i1_a29', 'p20002_i1_a30', 'p20002_i1_a31', 'p20002_i1_a32', 'p20002_i1_a33', 'p20002_i2_a0', 'p20002_i2_a1', 'p20002_i2_a2', 'p20002_i2_a3', 'p20002_i2_a4', 'p20002_i2_a5', 'p20002_i2_a6', 'p20002_i2_a7', 'p20002_i2_a8', 'p20002_i2_a9', 'p20002_i2_a10', 'p20002_i2_a11', 'p20002_i2_a12', 'p20002_i2_a13', 'p20002_i2_a14', 'p20002_i2_a15', 'p20002_i2_a16', 'p20002_i2_a17', 'p20002_i2_a18', 'p20002_i2_a19', 'p20002_i2_a20', 'p20002_i2_a21', 'p20002_i2_a22', 'p20002_i2_a23', 'p20002_i2_a24', 'p20002_i2_a25', 'p20002_i2_a26', 'p20002_i2_a27', 'p20002_i2_a28', 'p20002_i2_a29', 'p20002_i2_a30', 'p20002_i2_a31', 'p20002_i2_a32', 'p20002_i2_a33', 'p20002_i3_a0', 'p20002_i3_a1', 'p20002_i3_a2', 'p20002_i3_a3', 'p20002_i3_a4', 'p20002_i3_a5', 'p20002_i3_a6', 'p20002_i3_a7', 'p20002_i3_a8', 'p20002_i3_a9', 'p20002_i3_a10', 'p20002_i3_a11', 'p20002_i3_a12', 'p20002_i3_a13', 'p20002_i3_a14', 'p20002_i3_a15', 'p20002_i3_a16', 'p20002_i3_a17', 'p20002_i3_a18', 'p20002_i3_a19', 'p20002_i3_a20', 'p20002_i3_a21', 'p20002_i3_a22', 'p20002_i3_a23', 'p20002_i3_a24', 'p20002_i3_a25', 'p20002_i3_a26', 'p20002_i3_a27', 'p20002_i3_a28', 'p20002_i3_a29', 'p20002_i3_a30', 'p20002_i3_a31', 'p20002_i3_a32', 'p20002_i3_a33']



```python
df = participant.retrieve_fields(names=fields, engine=dxdata.connect(), coding_values="replace")

```


```python
# Generate the list of conditions for self-report fields
conditions = [f'(df.p20002_i{i}_a{j} == "eczema/dermatitis")' for i in range(4) for j in range(34)]

# Join the conditions with ' AND ' to create the final string
conditions_string = ' | '.join(conditions)

print(conditions_string)

```

    (df.p20002_i0_a0 == "eczema/dermatitis") | (df.p20002_i0_a1 == "eczema/dermatitis") | (df.p20002_i0_a2 == "eczema/dermatitis") | (df.p20002_i0_a3 == "eczema/dermatitis") | (df.p20002_i0_a4 == "eczema/dermatitis") | (df.p20002_i0_a5 == "eczema/dermatitis") | (df.p20002_i0_a6 == "eczema/dermatitis") | (df.p20002_i0_a7 == "eczema/dermatitis") | (df.p20002_i0_a8 == "eczema/dermatitis") | (df.p20002_i0_a9 == "eczema/dermatitis") | (df.p20002_i0_a10 == "eczema/dermatitis") | (df.p20002_i0_a11 == "eczema/dermatitis") | (df.p20002_i0_a12 == "eczema/dermatitis") | (df.p20002_i0_a13 == "eczema/dermatitis") | (df.p20002_i0_a14 == "eczema/dermatitis") | (df.p20002_i0_a15 == "eczema/dermatitis") | (df.p20002_i0_a16 == "eczema/dermatitis") | (df.p20002_i0_a17 == "eczema/dermatitis") | (df.p20002_i0_a18 == "eczema/dermatitis") | (df.p20002_i0_a19 == "eczema/dermatitis") | (df.p20002_i0_a20 == "eczema/dermatitis") | (df.p20002_i0_a21 == "eczema/dermatitis") | (df.p20002_i0_a22 == "eczema/dermatitis") | (df.p20002_i0_a23 == "eczema/dermatitis") | (df.p20002_i0_a24 == "eczema/dermatitis") | (df.p20002_i0_a25 == "eczema/dermatitis") | (df.p20002_i0_a26 == "eczema/dermatitis") | (df.p20002_i0_a27 == "eczema/dermatitis") | (df.p20002_i0_a28 == "eczema/dermatitis") | (df.p20002_i0_a29 == "eczema/dermatitis") | (df.p20002_i0_a30 == "eczema/dermatitis") | (df.p20002_i0_a31 == "eczema/dermatitis") | (df.p20002_i0_a32 == "eczema/dermatitis") | (df.p20002_i0_a33 == "eczema/dermatitis") | (df.p20002_i1_a0 == "eczema/dermatitis") | (df.p20002_i1_a1 == "eczema/dermatitis") | (df.p20002_i1_a2 == "eczema/dermatitis") | (df.p20002_i1_a3 == "eczema/dermatitis") | (df.p20002_i1_a4 == "eczema/dermatitis") | (df.p20002_i1_a5 == "eczema/dermatitis") | (df.p20002_i1_a6 == "eczema/dermatitis") | (df.p20002_i1_a7 == "eczema/dermatitis") | (df.p20002_i1_a8 == "eczema/dermatitis") | (df.p20002_i1_a9 == "eczema/dermatitis") | (df.p20002_i1_a10 == "eczema/dermatitis") | (df.p20002_i1_a11 == "eczema/dermatitis") | (df.p20002_i1_a12 == "eczema/dermatitis") | (df.p20002_i1_a13 == "eczema/dermatitis") | (df.p20002_i1_a14 == "eczema/dermatitis") | (df.p20002_i1_a15 == "eczema/dermatitis") | (df.p20002_i1_a16 == "eczema/dermatitis") | (df.p20002_i1_a17 == "eczema/dermatitis") | (df.p20002_i1_a18 == "eczema/dermatitis") | (df.p20002_i1_a19 == "eczema/dermatitis") | (df.p20002_i1_a20 == "eczema/dermatitis") | (df.p20002_i1_a21 == "eczema/dermatitis") | (df.p20002_i1_a22 == "eczema/dermatitis") | (df.p20002_i1_a23 == "eczema/dermatitis") | (df.p20002_i1_a24 == "eczema/dermatitis") | (df.p20002_i1_a25 == "eczema/dermatitis") | (df.p20002_i1_a26 == "eczema/dermatitis") | (df.p20002_i1_a27 == "eczema/dermatitis") | (df.p20002_i1_a28 == "eczema/dermatitis") | (df.p20002_i1_a29 == "eczema/dermatitis") | (df.p20002_i1_a30 == "eczema/dermatitis") | (df.p20002_i1_a31 == "eczema/dermatitis") | (df.p20002_i1_a32 == "eczema/dermatitis") | (df.p20002_i1_a33 == "eczema/dermatitis") | (df.p20002_i2_a0 == "eczema/dermatitis") | (df.p20002_i2_a1 == "eczema/dermatitis") | (df.p20002_i2_a2 == "eczema/dermatitis") | (df.p20002_i2_a3 == "eczema/dermatitis") | (df.p20002_i2_a4 == "eczema/dermatitis") | (df.p20002_i2_a5 == "eczema/dermatitis") | (df.p20002_i2_a6 == "eczema/dermatitis") | (df.p20002_i2_a7 == "eczema/dermatitis") | (df.p20002_i2_a8 == "eczema/dermatitis") | (df.p20002_i2_a9 == "eczema/dermatitis") | (df.p20002_i2_a10 == "eczema/dermatitis") | (df.p20002_i2_a11 == "eczema/dermatitis") | (df.p20002_i2_a12 == "eczema/dermatitis") | (df.p20002_i2_a13 == "eczema/dermatitis") | (df.p20002_i2_a14 == "eczema/dermatitis") | (df.p20002_i2_a15 == "eczema/dermatitis") | (df.p20002_i2_a16 == "eczema/dermatitis") | (df.p20002_i2_a17 == "eczema/dermatitis") | (df.p20002_i2_a18 == "eczema/dermatitis") | (df.p20002_i2_a19 == "eczema/dermatitis") | (df.p20002_i2_a20 == "eczema/dermatitis") | (df.p20002_i2_a21 == "eczema/dermatitis") | (df.p20002_i2_a22 == "eczema/dermatitis") | (df.p20002_i2_a23 == "eczema/dermatitis") | (df.p20002_i2_a24 == "eczema/dermatitis") | (df.p20002_i2_a25 == "eczema/dermatitis") | (df.p20002_i2_a26 == "eczema/dermatitis") | (df.p20002_i2_a27 == "eczema/dermatitis") | (df.p20002_i2_a28 == "eczema/dermatitis") | (df.p20002_i2_a29 == "eczema/dermatitis") | (df.p20002_i2_a30 == "eczema/dermatitis") | (df.p20002_i2_a31 == "eczema/dermatitis") | (df.p20002_i2_a32 == "eczema/dermatitis") | (df.p20002_i2_a33 == "eczema/dermatitis") | (df.p20002_i3_a0 == "eczema/dermatitis") | (df.p20002_i3_a1 == "eczema/dermatitis") | (df.p20002_i3_a2 == "eczema/dermatitis") | (df.p20002_i3_a3 == "eczema/dermatitis") | (df.p20002_i3_a4 == "eczema/dermatitis") | (df.p20002_i3_a5 == "eczema/dermatitis") | (df.p20002_i3_a6 == "eczema/dermatitis") | (df.p20002_i3_a7 == "eczema/dermatitis") | (df.p20002_i3_a8 == "eczema/dermatitis") | (df.p20002_i3_a9 == "eczema/dermatitis") | (df.p20002_i3_a10 == "eczema/dermatitis") | (df.p20002_i3_a11 == "eczema/dermatitis") | (df.p20002_i3_a12 == "eczema/dermatitis") | (df.p20002_i3_a13 == "eczema/dermatitis") | (df.p20002_i3_a14 == "eczema/dermatitis") | (df.p20002_i3_a15 == "eczema/dermatitis") | (df.p20002_i3_a16 == "eczema/dermatitis") | (df.p20002_i3_a17 == "eczema/dermatitis") | (df.p20002_i3_a18 == "eczema/dermatitis") | (df.p20002_i3_a19 == "eczema/dermatitis") | (df.p20002_i3_a20 == "eczema/dermatitis") | (df.p20002_i3_a21 == "eczema/dermatitis") | (df.p20002_i3_a22 == "eczema/dermatitis") | (df.p20002_i3_a23 == "eczema/dermatitis") | (df.p20002_i3_a24 == "eczema/dermatitis") | (df.p20002_i3_a25 == "eczema/dermatitis") | (df.p20002_i3_a26 == "eczema/dermatitis") | (df.p20002_i3_a27 == "eczema/dermatitis") | (df.p20002_i3_a28 == "eczema/dermatitis") | (df.p20002_i3_a29 == "eczema/dermatitis") | (df.p20002_i3_a30 == "eczema/dermatitis") | (df.p20002_i3_a31 == "eczema/dermatitis") | (df.p20002_i3_a32 == "eczema/dermatitis") | (df.p20002_i3_a33 == "eczema/dermatitis")



```python
self = df.filter((df.p20002_i0_a0 == "eczema/dermatitis") | (df.p20002_i0_a1 == "eczema/dermatitis") | (df.p20002_i0_a2 == "eczema/dermatitis") | (df.p20002_i0_a3 == "eczema/dermatitis") | (df.p20002_i0_a4 == "eczema/dermatitis") | (df.p20002_i0_a5 == "eczema/dermatitis") | (df.p20002_i0_a6 == "eczema/dermatitis") | (df.p20002_i0_a7 == "eczema/dermatitis") | (df.p20002_i0_a8 == "eczema/dermatitis") | (df.p20002_i0_a9 == "eczema/dermatitis") | (df.p20002_i0_a10 == "eczema/dermatitis") | (df.p20002_i0_a11 == "eczema/dermatitis") | (df.p20002_i0_a12 == "eczema/dermatitis") | (df.p20002_i0_a13 == "eczema/dermatitis") | (df.p20002_i0_a14 == "eczema/dermatitis") | (df.p20002_i0_a15 == "eczema/dermatitis") | (df.p20002_i0_a16 == "eczema/dermatitis") | (df.p20002_i0_a17 == "eczema/dermatitis") | (df.p20002_i0_a18 == "eczema/dermatitis") | (df.p20002_i0_a19 == "eczema/dermatitis") | (df.p20002_i0_a20 == "eczema/dermatitis") | (df.p20002_i0_a21 == "eczema/dermatitis") | (df.p20002_i0_a22 == "eczema/dermatitis") | (df.p20002_i0_a23 == "eczema/dermatitis") | (df.p20002_i0_a24 == "eczema/dermatitis") | (df.p20002_i0_a25 == "eczema/dermatitis") | (df.p20002_i0_a26 == "eczema/dermatitis") | (df.p20002_i0_a27 == "eczema/dermatitis") | (df.p20002_i0_a28 == "eczema/dermatitis") | (df.p20002_i0_a29 == "eczema/dermatitis") | (df.p20002_i0_a30 == "eczema/dermatitis") | (df.p20002_i0_a31 == "eczema/dermatitis") | (df.p20002_i0_a32 == "eczema/dermatitis") | (df.p20002_i0_a33 == "eczema/dermatitis") | (df.p20002_i1_a0 == "eczema/dermatitis") | (df.p20002_i1_a1 == "eczema/dermatitis") | (df.p20002_i1_a2 == "eczema/dermatitis") | (df.p20002_i1_a3 == "eczema/dermatitis") | (df.p20002_i1_a4 == "eczema/dermatitis") | (df.p20002_i1_a5 == "eczema/dermatitis") | (df.p20002_i1_a6 == "eczema/dermatitis") | (df.p20002_i1_a7 == "eczema/dermatitis") | (df.p20002_i1_a8 == "eczema/dermatitis") | (df.p20002_i1_a9 == "eczema/dermatitis") | (df.p20002_i1_a10 == "eczema/dermatitis") | (df.p20002_i1_a11 == "eczema/dermatitis") | (df.p20002_i1_a12 == "eczema/dermatitis") | (df.p20002_i1_a13 == "eczema/dermatitis") | (df.p20002_i1_a14 == "eczema/dermatitis") | (df.p20002_i1_a15 == "eczema/dermatitis") | (df.p20002_i1_a16 == "eczema/dermatitis") | (df.p20002_i1_a17 == "eczema/dermatitis") | (df.p20002_i1_a18 == "eczema/dermatitis") | (df.p20002_i1_a19 == "eczema/dermatitis") | (df.p20002_i1_a20 == "eczema/dermatitis") | (df.p20002_i1_a21 == "eczema/dermatitis") | (df.p20002_i1_a22 == "eczema/dermatitis") | (df.p20002_i1_a23 == "eczema/dermatitis") | (df.p20002_i1_a24 == "eczema/dermatitis") | (df.p20002_i1_a25 == "eczema/dermatitis") | (df.p20002_i1_a26 == "eczema/dermatitis") | (df.p20002_i1_a27 == "eczema/dermatitis") | (df.p20002_i1_a28 == "eczema/dermatitis") | (df.p20002_i1_a29 == "eczema/dermatitis") | (df.p20002_i1_a30 == "eczema/dermatitis") | (df.p20002_i1_a31 == "eczema/dermatitis") | (df.p20002_i1_a32 == "eczema/dermatitis") | (df.p20002_i1_a33 == "eczema/dermatitis") | (df.p20002_i2_a0 == "eczema/dermatitis") | (df.p20002_i2_a1 == "eczema/dermatitis") | (df.p20002_i2_a2 == "eczema/dermatitis") | (df.p20002_i2_a3 == "eczema/dermatitis") | (df.p20002_i2_a4 == "eczema/dermatitis") | (df.p20002_i2_a5 == "eczema/dermatitis") | (df.p20002_i2_a6 == "eczema/dermatitis") | (df.p20002_i2_a7 == "eczema/dermatitis") | (df.p20002_i2_a8 == "eczema/dermatitis") | (df.p20002_i2_a9 == "eczema/dermatitis") | (df.p20002_i2_a10 == "eczema/dermatitis") | (df.p20002_i2_a11 == "eczema/dermatitis") | (df.p20002_i2_a12 == "eczema/dermatitis") | (df.p20002_i2_a13 == "eczema/dermatitis") | (df.p20002_i2_a14 == "eczema/dermatitis") | (df.p20002_i2_a15 == "eczema/dermatitis") | (df.p20002_i2_a16 == "eczema/dermatitis") | (df.p20002_i2_a17 == "eczema/dermatitis") | (df.p20002_i2_a18 == "eczema/dermatitis") | (df.p20002_i2_a19 == "eczema/dermatitis") | (df.p20002_i2_a20 == "eczema/dermatitis") | (df.p20002_i2_a21 == "eczema/dermatitis") | (df.p20002_i2_a22 == "eczema/dermatitis") | (df.p20002_i2_a23 == "eczema/dermatitis") | (df.p20002_i2_a24 == "eczema/dermatitis") | (df.p20002_i2_a25 == "eczema/dermatitis") | (df.p20002_i2_a26 == "eczema/dermatitis") | (df.p20002_i2_a27 == "eczema/dermatitis") | (df.p20002_i2_a28 == "eczema/dermatitis") | (df.p20002_i2_a29 == "eczema/dermatitis") | (df.p20002_i2_a30 == "eczema/dermatitis") | (df.p20002_i2_a31 == "eczema/dermatitis") | (df.p20002_i2_a32 == "eczema/dermatitis") | (df.p20002_i2_a33 == "eczema/dermatitis") | (df.p20002_i3_a0 == "eczema/dermatitis") | (df.p20002_i3_a1 == "eczema/dermatitis") | (df.p20002_i3_a2 == "eczema/dermatitis") | (df.p20002_i3_a3 == "eczema/dermatitis") | (df.p20002_i3_a4 == "eczema/dermatitis") | (df.p20002_i3_a5 == "eczema/dermatitis") | (df.p20002_i3_a6 == "eczema/dermatitis") | (df.p20002_i3_a7 == "eczema/dermatitis") | (df.p20002_i3_a8 == "eczema/dermatitis") | (df.p20002_i3_a9 == "eczema/dermatitis") | (df.p20002_i3_a10 == "eczema/dermatitis") | (df.p20002_i3_a11 == "eczema/dermatitis") | (df.p20002_i3_a12 == "eczema/dermatitis") | (df.p20002_i3_a13 == "eczema/dermatitis") | (df.p20002_i3_a14 == "eczema/dermatitis") | (df.p20002_i3_a15 == "eczema/dermatitis") | (df.p20002_i3_a16 == "eczema/dermatitis") | (df.p20002_i3_a17 == "eczema/dermatitis") | (df.p20002_i3_a18 == "eczema/dermatitis") | (df.p20002_i3_a19 == "eczema/dermatitis") | (df.p20002_i3_a20 == "eczema/dermatitis") | (df.p20002_i3_a21 == "eczema/dermatitis") | (df.p20002_i3_a22 == "eczema/dermatitis") | (df.p20002_i3_a23 == "eczema/dermatitis") | (df.p20002_i3_a24 == "eczema/dermatitis") | (df.p20002_i3_a25 == "eczema/dermatitis") | (df.p20002_i3_a26 == "eczema/dermatitis") | (df.p20002_i3_a27 == "eczema/dermatitis") | (df.p20002_i3_a28 == "eczema/dermatitis") | (df.p20002_i3_a29 == "eczema/dermatitis") | (df.p20002_i3_a30 == "eczema/dermatitis") | (df.p20002_i3_a31 == "eczema/dermatitis") | (df.p20002_i3_a32 == "eczema/dermatitis") | (df.p20002_i3_a33 == "eczema/dermatitis"))
```


```python
self_pd = self.toPandas()
```

    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series



```python
self_pd.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>eid</th>
      <th>p41271</th>
      <th>p41270</th>
      <th>p41202</th>
      <th>p41203</th>
      <th>p41204</th>
      <th>p41205</th>
      <th>p20002_i0_a0</th>
      <th>p20002_i0_a1</th>
      <th>p20002_i0_a2</th>
      <th>...</th>
      <th>p20002_i3_a24</th>
      <th>p20002_i3_a25</th>
      <th>p20002_i3_a26</th>
      <th>p20002_i3_a27</th>
      <th>p20002_i3_a28</th>
      <th>p20002_i3_a29</th>
      <th>p20002_i3_a30</th>
      <th>p20002_i3_a31</th>
      <th>p20002_i3_a32</th>
      <th>p20002_i3_a33</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2108226</td>
      <td>None</td>
      <td>[D40.1 Testis, I10 Essential (primary) hyperte...</td>
      <td>[D40.1 Testis, J18.1 Lobar pneumonia, unspecif...</td>
      <td>None</td>
      <td>[I10 Essential (primary) hypertension, J90 Ple...</td>
      <td>None</td>
      <td>hypertension</td>
      <td>high cholesterol</td>
      <td>eczema/dermatitis</td>
      <td>...</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2407835</td>
      <td>None</td>
      <td>[B35.6 Tinea cruris, F10.0 Acute intoxication,...</td>
      <td>[B35.6 Tinea cruris, S01.8 Open wound of other...</td>
      <td>None</td>
      <td>[F10.0 Acute intoxication, F32.9 Depressive ep...</td>
      <td>None</td>
      <td>asthma</td>
      <td>depression</td>
      <td>None</td>
      <td>...</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1199035</td>
      <td>None</td>
      <td>[F10.1 Harmful use, K22.2 Oesophageal obstruct...</td>
      <td>[R13 Dysphagia, T39.1 4-Aminophenol derivatives]</td>
      <td>None</td>
      <td>[F10.1 Harmful use, K22.2 Oesophageal obstruct...</td>
      <td>None</td>
      <td>unclassifiable</td>
      <td>eczema/dermatitis</td>
      <td>gastro-oesophageal reflux (gord) / gastric reflux</td>
      <td>...</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>3</th>
      <td>1456128</td>
      <td>[2165 Benign neoplasm of skin of trunk, except...</td>
      <td>[E11.9 Without complications, G47.3 Sleep apno...</td>
      <td>[G47.3 Sleep apnoea, G47.9 Sleep disorder, uns...</td>
      <td>[2165 Benign neoplasm of skin of trunk, except...</td>
      <td>[E11.9 Without complications, G47.3 Sleep apno...</td>
      <td>[5953 Trigonitis, 7881 Dysuria, 7884 Frequency...</td>
      <td>hypertension</td>
      <td>diabetes</td>
      <td>kidney stone/ureter stone/bladder stone</td>
      <td>...</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>4</th>
      <td>3320817</td>
      <td>None</td>
      <td>[C61 Malignant neoplasm of prostate, D23.7 Ski...</td>
      <td>[C61 Malignant neoplasm of prostate, K40.9 Uni...</td>
      <td>None</td>
      <td>[C61 Malignant neoplasm of prostate, D23.7 Ski...</td>
      <td>None</td>
      <td>eczema/dermatitis</td>
      <td>high cholesterol</td>
      <td>eye/eyelid problem</td>
      <td>...</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
    </tr>
  </tbody>
</table>
<p>5 rows × 143 columns</p>
</div>




```python
from pyspark.sql.functions import array_contains
icd10 = df.filter(array_contains(df.p41270,"L20 Atopic dermatitis") | array_contains(df.p41270,"L20.8 Other atopic dermatitis") | array_contains(df.p41270,"L20.9 Atopic dermatitis, unspecified"))
icd9 = df.filter(array_contains(df.p41271,"691 Atopic dermatitis and related conditions") | array_contains(df.p41271,"6918 Other atopic dermatitis and related conditions") | array_contains(df.p41271,"69180 Atopic dermatitis"))
```


```python
icd10_pd = icd10.toPandas()
icd9_pd = icd9.toPandas()
```

    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series
    /cluster/spark/python/pyspark/sql/pandas/conversion.py:202: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
      df[column_name] = series



```python
# collapse self_pd dataframe so that it's just a column with 1 under 'self-report' 

self_report = pd.DataFrame(self_pd['eid'])
self_report['self_report'] = 1
```


```python
self_report.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>eid</th>
      <th>self_report</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2108226</td>
      <td>1</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2407835</td>
      <td>1</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1199035</td>
      <td>1</td>
    </tr>
    <tr>
      <th>3</th>
      <td>1456128</td>
      <td>1</td>
    </tr>
    <tr>
      <th>4</th>
      <td>3320817</td>
      <td>1</td>
    </tr>
  </tbody>
</table>
</div>




```python
icd10_new = pd.DataFrame(icd10_pd['eid'])
icd10_new['self_report'] = 1

icd9_new = pd.DataFrame(icd9_pd['eid'])
icd9_new['self_report'] = 1
```


```python
merged_df = self_report.merge(icd10_new, on='eid', how='outer').merge(icd9_new, on='eid', how='outer')
```


```python
merged_df.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>eid</th>
      <th>self_report_x</th>
      <th>self_report_y</th>
      <th>self_report</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2108226</td>
      <td>1.0</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2407835</td>
      <td>1.0</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1199035</td>
      <td>1.0</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>3</th>
      <td>1456128</td>
      <td>1.0</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>4</th>
      <td>3320817</td>
      <td>1.0</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
  </tbody>
</table>
</div>




```python
merged_df = merged_df.fillna(0)
```


```python
merged_df.to_csv('merged_data.csv', index=False)
```


```bash
%%bash
dx upload merged_data.csv --dest /
```

    ID                                file-Gq1k13jJYjP06jYpQZ70P3Vq
    Class                             file
    Project                           project-GfgxgX0JYjP7Q7Jj8Bg7xVyz
    Folder                            /
    Name                              merged_data.csv
    State                             closing
    Visibility                        visible
    Types                             -
    Properties                        -
    Tags                              -
    Outgoing links                    -
    Created                           Mon Aug 19 18:35:59 2024
    Created by                        s_shen
     via the job                      job-Gq1j2JjJYjP057Z0bfvJ786f
    Last modified                     Mon Aug 19 18:36:00 2024
    Media type                        
    archivalState                     "live"
    cloudAccount                      "cloudaccount-dnanexus"

