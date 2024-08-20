```python
import pyspark
import dxpy
import dxdata
import pandas as pd

# Spark initialization (Done only once; do not rerun this cell unless you select Kernel -> Restart kernel).
sc = pyspark.SparkContext()
spark = pyspark.sql.SparkSession(sc)
```


```python
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
```


```python
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
test = df.filter(df.eid == '2556115')
```


```python
test_pd = test.toPandas()
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
test_pd['p41270'].str.contains('C61')
```




    0   NaN
    Name: p41270, dtype: float64




```python
test_pd['p41204'] = test_pd['p41204'].astype(str)
```


```python
result = test_pd['p41204'].str.contains('L2')
print(result)
```

    0    False
    Name: p41204, dtype: bool



```python
test_pd
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
      <td>2556115</td>
      <td>None</td>
      <td>['C61 Malignant neoplasm of prostate', 'K40.9 ...</td>
      <td>[C61 Malignant neoplasm of prostate, K40.9 Uni...</td>
      <td>None</td>
      <td>[Z85.4 Personal history of malignant neoplasm ...</td>
      <td>None</td>
      <td>inguinal hernia</td>
      <td>None</td>
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
  </tbody>
</table>
<p>1 rows × 143 columns</p>
</div>




```python
# The string you're searching for
search_string = 'ecz'

# Stack the DataFrame to reshape it into a Series
stacked = test_pd.stack()

# Filter the stacked DataFrame to find where the string is found
result = stacked[stacked.str.contains(search_string, na=False)]

# Reset the index to get the row and column information
result = result.reset_index()

# Rename the columns for clarity
result.columns = ['Row', 'Column', 'Value']

print(result)
```

    Empty DataFrame
    Columns: [Row, Column, Value]
    Index: []



```python

```
