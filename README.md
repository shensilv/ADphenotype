# AD phenotype
Code for Chapter 2 of my PhD thesis: "Refining the Atopic Dermatitis phenotype"

For more info on UKB RAP see: https://github.com/UK-Biobank/UKB-RAP-Notebooks/tree/main  
Special thanks to the creators of this tutorial: https://github.com/statgenetics/UKBB_GWAS_dev/blob/master/RAP_UKBB.ipynb


- To extract the phenotype data for baseline AD phenotype, follow the instruction here: [extract_AD_RAP.md](extract_AD_RAP.md)   
- To extract individuals to be excluded from analysis, follow the instructions here: [extract_exclusions.md](extract_exclusions.md).   
- The above code will download a list of eids and the associated field values in a csv (or tsv) format. To create the phenotype file used in programmes such as plink/gcta, follow instructions here: [create_phenfile.md](create_phenfile.md). 

The file [phen_extraction.md](phen_extraction.md) is the phenotype extraction file used for project 81499. 
