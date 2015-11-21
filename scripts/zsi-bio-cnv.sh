#!/bin/sh

echo
echo "...::: ZSI-BIO-CNV :::..."
echo

print_usage() {
    echo "Main script arguments:

  --jar
    Path to jar file containing ZSI-BIO-CNV project.
    By default it points to current folder and assumes that the jar file name is zsi-bio-cnv.jar.

  --conf
    Path to file containing additional Spark configuration.
    By default it points to current folder and assumes that the file name is application.conf.

  --app
    Name of module you want to run.
    Must be one of: Coverage | DepthOfCoverage | Conifer.

Arguments for Coverage:

  --bedFile
    Path to folder containing BED file.

  --bamFiles
    Path to folder containing BAM files.

  --output
    Path to file with the result.

Arguments for DepthOfCoverage:

  --bedFile
    Path to folder containing BED file.

  --bamFiles
    Path to folder containing BAM files.

  --output
    Path to file with the result.

Arguments for Conifer:

  --bedFile
    Path to folder containing BED file.

  --bamFiles
    Path to folder containing BAM files.

  --minMedian
    Minimum population median RPKM per probe.
    Default value: 1.0.

  --svd
    Number of components to remove.
    Default value: 12.

  --threshold
    +/- threshold for calling (minimum SVD-ZRPKM).
    Default value: 1.5.

  --output
    Path to file with the result."
}

checkArgumentsAndRunCoverage() {
    if [ -z ${bedFile} ]; then
        echo "Missing argument: --bedFile. Exiting ..."
    elif [ -z ${bamFiles} ]; then
        echo "Missing argument: --bamFiles. Exiting ..."
    elif [ -z ${output} ]; then
        echo "Missing argument: --output. Exiting ..."
    else
        spark-submit --properties-file ${conf} ${jar} ${app} ${bedFile} ${bamFiles} ${output}
    fi
}

checkArgumentsAndRunDepthOfCoverage() {
    if [ -z ${bedFile} ]; then
        echo "Missing argument: --bedFile. Exiting!"
    elif [ -z ${bamFiles} ]; then
        echo "Missing argument: --bamFiles. Exiting!"
    elif [ -z ${output} ]; then
        echo "Missing argument: --output. Exiting!"
    else
        spark-submit --properties-file ${conf} ${jar} ${app} ${bedFile} ${bamFiles} ${output}
    fi
}

checkArgumentsAndRunConifer() {
    if [ -z ${bedFile} ]; then
        echo "Missing argument: --bedFile. Exiting!"
    elif [ -z ${bamFiles} ]; then
        echo "Missing argument: --bamFiles. Exiting!"
    elif [ -z ${output} ]; then
        echo "Missing argument: --output. Exiting!"
    else
        if [ -z ${minMedian} ]; then
            echo "Missing argument: --minMedian. Using default value: 1.0!"
            minMedian=1.0
        fi
        if [ -z ${svd} ]; then
            echo "Missing argument: --svd. Using default value: 12!"
            svd=12
        fi
        if [ -z ${threshold} ]; then
            echo "Missing argument: --threshold. Using default value: 1.5!"
            threshold=1.5
        fi
        spark-submit --properties-file ${conf} ${jar} ${app} ${bedFile} ${bamFiles} ${minMedian} ${svd} ${threshold} ${output}
    fi
}

loadArguments() {
    for param in $@; do
        case $param in
            --jar=*) jar="${param#*=}" ;;
            --conf=*) conf="${param#*=}" ;;
            --app=*) app="${param#*=}" ;;
            --bedFile=*) bedFile="${param#*=}" ;;
            --bamFiles=*) bamFiles="${param#*=}";;
            --minMedian=*) minMedian="${param#*=}" ;;
            --svd=*) svd="${param#*=}" ;;
            --threshold=*) threshold="${param#*=}" ;;
            --output=*) output="${param#*=}" ;;
            *) echo "Unknown argument: $param. Skipping!" ;;
        esac
    done
}

setDefaults() {
    if [ -z ${jar} ]; then
        echo "Missing argument: --jar. Using default value: `pwd`/zsi-bio-cnv.jar!"
        jar="`pwd`/zsi-bio-cnv.jar"
    fi
    if [ -z ${conf} ]; then
        echo "Missing argument: --conf. Using default value: `pwd`/application.conf!"
        conf="`pwd`/application.conf"
    fi
}

if [ $# -le 0 ]; then
    print_usage
else
    loadArguments $@
    setDefaults
    if [ -z ${app} ]; then
        echo "Missing argument: --app. Exiting!"
    elif [ ${app} = "Coverage" ]; then
        checkArgumentsAndRunCoverage
    elif [ ${app} = "DepthOfCoverage" ]; then
        checkArgumentsAndRunDepthOfCoverage
    elif [ ${app} = "Conifer" ]; then
        checkArgumentsAndRunConifer
    else
        echo "Unknown application mode: ${appMode}."
        echo "Must be one of: Coverage, DepthOfCoverage, Conifer."
    fi
fi