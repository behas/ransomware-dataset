now=$(date +"%m_%d_%Y")
filename=ransomware_dataset_$now.tar.gz

if [ -f $filename ]
then
    echo "$filename already exists"
else
    echo "Packing dump files into $filename"
    tar cvfz $filename ./dataset/blockchain/*.csv
fi

gsutil cp $filename gs://graphsense-dumps/ransomware/
