# Create the binary phenotype file for baseline atopic dermatitis 

After downloading GP read2 and read3, ICD9/10 and self-reported conditions data from [extract_AD_RAP.md](extract_AD_RAP.md), we now create a GCTA-readable phenotype file. This analysis is done in R and bash. 

We have three files:  
1. GP data:
| eid | event_dt | read_2 | read_3 |
|------|-----|-----|
