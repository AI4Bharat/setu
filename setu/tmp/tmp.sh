for i in "2" "3" "4" "5";
do 
    gsutil -m cp -r gs://sangraha/parquets/hindi/$i /data/priyam/sangraha/parquets/hindi/ 
done