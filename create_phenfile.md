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

## Extract ICD10 case eids
```
%%bash
# count number of ICD10 cases - 281
grep -E 'L20|L20\.8|L20\.9' AD_icd_self.csv | wc -l
grep -E 'L20|L20\.8|L20\.9' AD_icd_self.csv | awk 'BEGIN{FS=","}{print $1}' >> ICD10_eids.txt
```

## Extract ICD9 case eids 
```
%%bash
# count number of ICD9 cases - 23
grep -E '691 Atopic dermatitis and related conditions|6918 Other atopic dermatitis and related conditions|69180 Atopic dermatitis' AD_icd_self.csv | wc -l
grep -E '691 Atopic dermatitis and related conditions|6918 Other atopic dermatitis and related conditions|69180 Atopic dermatitis' AD_icd_self.csv | awk 'BEGIN{FS=","}{print $1}' >> ICD9_eids.txt
```

## Extract self-report case eids
```
%%bash
# count number of self-report cases - 16045
grep 'eczema/dermatitis' AD_icd_self.csv | wc -l
grep 'eczema/dermatitis' AD_icd_self.csv | awk 'BEGIN{FS=","}{print $1}' >> selfreport_eids.txt
```

## Extract GP case eids
```
%%bash
# grepping on Read2 and Read3 gives non-unique entries -- 44,749
grep -E 'M11\.\.|M111\.|M112\.|M113\.|M114\.|M115\.|M11z\.|M12z1|Myu22|X505j|XaINM|XE1Av|XE1C6' gp_records.tsv | wc -l
# unique eids: 26939
grep -E 'M11\.\.|M111\.|M112\.|M113\.|M114\.|M115\.|M11z\.|M12z1|Myu22|X505j|XaINM|XE1Av|XE1C6' gp_records.tsv | awk 'BEGIN{FS="\t"}{print $1}' | sort | uniq >> gp_eids.txt
```

## Create binary phenotype file and remove exclusions
```
%%bash
cat gp_eids.txt >> AD_case_eids.txt
cat selfreport_eids.txt >> AD_case_eids.txt
cat ICD9_eids.txt >> AD_case_eids.txt
cat ICD10_eids.txt >> AD_case_eids.txt
# number of individuals 40083
cat AD_case_eids.txt | sort | uniq >> AD_cases.txt
```

```
# IN R
library(data.table)
library(dplyr)

full_eids <- fread("/Users/s2225464/Documents/scripts/ECZ_heritability/UKB_reml/make_phenotype/full_eids.txt", header=FALSE)
AD_eids <- fread("/Users/s2225464/Documents/scripts/ECZ_heritability/UKB_reml/make_phenotype/AD_cases.txt", header=FALSE)

exclusions <- fread("/Users/s2225464/Documents/scripts/ECZ_heritability/UKB_reml/make_phenotype/full_excl.txt", header=TRUE)

phenotype <- full_eids %>%
  mutate(AD_status = ifelse(V1 %in% AD_eids$V1, 1, 0))

phenotype <- phenotype %>%
  filter(!(V1 %in% exclusions$eid))

colnames(phenotype) <- c("eid", "sex", "AD_status")

write.table(phenotype, "/Users/s2225464/Documents/ECZ_heritability/UKB_reml/make_phenotype/ad_phen_sex.tsv", sep = "\t", row.names = FALSE, col.names = TRUE, quote=FALSE)
```
The file `ad_phen_sex.tsv' is the final AD phenotype with exclusions removed for all individuals in UKB. 
