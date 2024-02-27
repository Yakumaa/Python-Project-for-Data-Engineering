echo "extract_transfrom_and_load"
cut -d":" -f1,3,6 /etc/passwd > /home/shrish/airflow/dags/extracted-data.txt

tr ":" "," < /home/shirsh/airflow/dags/extracted-data.txt > /home/shrish/airflow/dags/transformed-data.csv