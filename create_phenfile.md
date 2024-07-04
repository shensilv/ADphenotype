# Create the binary phenotype file for baseline atopic dermatitis 

After downloading GP read2 and read3, ICD9/10 and self-reported conditions data from [extract_AD_RAP.md](extract_AD_RAP.md), we now create a GCTA-readable phenotype file. This analysis is done in R and bash. 

We have three files:  
1. GP data: Each line is a record at a specific date, as the example table shown below. The total number of lines in this file (for all UKB participants) is 118,226,523.

| eid | event_dt | read_2 | read_3 |  
|----------|----------|----------|----------|
| 000000 | 2001-07-11 | 8H4C. ||  
| 000000 | 2005-07-11 || XE2JU |  

2. ICD9/10 data and self-report data
This is in a huge file with annoying comma-separation (better to extract tab-separated). Better to filter these via grep and awk.

```
%%bash
