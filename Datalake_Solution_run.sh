spark2-submit \
   --master yarn \
   --deploy-mode cluster \
   --driver-memory 8g \
   --executor-memory 16g \
   --executor-cores 2  \
   --py-files Newyorker_Datalake_Solution _with_run_script.py $1 $2 $3 $4 $5