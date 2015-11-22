#!/bin/bash

data="/path/to/data"
results="/path/to/results"

start=$SECONDS
echo "Starting step 1"

(java -Xmx3072m -jar $data/GenomeAnalysisTK.jar -T DepthOfCoverage \
-I $data/group1.READS.bam.list -L $data/EXOME.interval_list \
-R $data/human_g1k_v37.fasta -dt BY_SAMPLE -dcov 5000 -l INFO \
--omitDepthOutputAtEachBase --omitLocusTable --minBaseQuality 0 \
--minMappingQuality 20 --start 1 --stop 5000 --nBins 200 --includeRefNSites \
--countType COUNT_READS -o $results/group1.DATA) &> /dev/null &

(java -Xmx3072m -jar $data/GenomeAnalysisTK.jar -T DepthOfCoverage \
-I $data/group2.READS.bam.list -L $data/EXOME.interval_list \
-R $data/human_g1k_v37.fasta -dt BY_SAMPLE -dcov 5000 -l INFO \
--omitDepthOutputAtEachBase --omitLocusTable --minBaseQuality 0 \
--minMappingQuality 20 --start 1 --stop 5000 --nBins 200 --includeRefNSites \
--countType COUNT_READS -o $results/group2.DATA) &> /dev/null &

(java -Xmx3072m -jar $data/GenomeAnalysisTK.jar -T DepthOfCoverage \
-I $data/group3.READS.bam.list -L $data/EXOME.interval_list \
-R $data/human_g1k_v37.fasta -dt BY_SAMPLE -dcov 5000 -l INFO \
--omitDepthOutputAtEachBase --omitLocusTable --minBaseQuality 0 \
--minMappingQuality 20 --start 1 --stop 5000 --nBins 200 --includeRefNSites \
--countType COUNT_READS -o $results/group3.DATA) &> /dev/null &

wait

echo "Finishing step 1"
step1=$SECONDS
echo "Starting step 2"

(./xhmm --mergeGATKdepths -o $results/DATA.RD.txt \
--GATKdepths $results/group1.DATA.sample_interval_summary \
--GATKdepths $results/group2.DATA.sample_interval_summary \
--GATKdepths $results/group3.DATA.sample_interval_summary) \
&> /dev/null

echo "Finishing step 2"
step2=$SECONDS
echo "Starting step 3"

(./xhmm --matrix -r $results/DATA.RD.txt --centerData \
--centerType target -o $results/DATA.filtered_centered.RD.txt \
--outputExcludedTargets $results/DATA.filtered_centered.RD.txt.filtered_targets.txt \
--outputExcludedSamples $results/DATA.filtered_centered.RD.txt.filtered_samples.txt \
--minTargetSize 10 --maxTargetSize 10000 --minMeanTargetRD 10 --maxMeanTargetRD 500 \
--minMeanSampleRD 25 --maxMeanSampleRD 200 --maxSdSampleRD 150) &> /dev/null

echo "Finishing step 3"
step3=$SECONDS
echo "Starting step 4"

(./xhmm --PCA -r $results/DATA.filtered_centered.RD.txt \
--PCAfiles $results/DATA.RD_PCA) &> /dev/null

echo "Finishing step 4"
step4=$SECONDS
echo "Starting step 5"

(./xhmm --normalize -r $results/DATA.filtered_centered.RD.txt \
--PCAfiles $results/DATA.RD_PCA --normalizeOutput $results/DATA.PCA_normalized.txt \
--PCnormalizeMethod PVE_mean --PVE_mean_factor 0.7) &> /dev/null

echo "Finishing step 5"
step5=$SECONDS
echo "Starting step 6"

(./xhmm --matrix -r $results/DATA.PCA_normalized.txt --centerData --centerType sample \
--zScoreData -o $results/DATA.PCA_normalized.filtered.sample_zscores.RD.txt \
--outputExcludedTargets $results/DATA.PCA_normalized.filtered.sample_zscores.RD.txt.filtered_targets.txt \
--outputExcludedSamples $results/DATA.PCA_normalized.filtered.sample_zscores.RD.txt.filtered_samples.txt \
--maxSdTargetRD 30) &> /dev/null

echo "Finishing step 6"
step6=$SECONDS
echo "Starting step 7"

(./xhmm --matrix -r $results/DATA.RD.txt \
--excludeTargets $results/DATA.filtered_centered.RD.txt.filtered_targets.txt \
--excludeTargets $results/DATA.PCA_normalized.filtered.sample_zscores.RD.txt.filtered_targets.txt \
--excludeSamples $results/DATA.filtered_centered.RD.txt.filtered_samples.txt \
--excludeSamples $results/DATA.PCA_normalized.filtered.sample_zscores.RD.txt.filtered_samples.txt \
-o $results/DATA.same_filtered.RD.txt) &> /dev/null

echo "Finishing step 7"
step7=$SECONDS
echo "Starting step 8"

(./xhmm --discover -p $data/params.txt \
-r $results/DATA.PCA_normalized.filtered.sample_zscores.RD.txt \
-R $results/DATA.same_filtered.RD.txt -c $results/DATA.xcnv \
-a $results/DATA.aux_xcnv -s $results/DATA) &> /dev/null

echo "Finishing step 8"
step8=$SECONDS
echo "Starting step 9"

(./xhmm --genotype -p $data/params.txt \
-r $results/DATA.PCA_normalized.filtered.sample_zscores.RD.txt \
-R $results/DATA.same_filtered.RD.txt -g $results/DATA.xcnv \
-F $data/human_g1k_v37.fasta -v $results/DATA.vcf) &> /dev/null

echo "Finishing step 9"
step9=$SECONDS

echo "Execution times:
Step #1: $(($step1 - $start)) sec
Step #2: $(($step2 - $step1)) sec
Step #3: $(($step3 - $step2)) sec
Step #4: $(($step4 - $step3)) sec
Step #5: $(($step5 - $step4)) sec
Step #6: $(($step6 - $step5)) sec
Step #7: $(($step7 - $step6)) sec
Step #8: $(($step8 - $step7)) sec
Step #9: $(($step9 - $step8)) sec
" > $results/times.txt
