cut -f1,1,4 -d"#" web-server-access-log.txt > /home/shrish/airflow/dags/ETL_extracted.txt

tr "[a-z]" "[A-Z]" < /home/shrish/airflow/dags/ETL_extracted.txt > /home/shrish/airflow/dags/ETL_capitalized.txt

tar -czvf /home/shrish/airflow/dags/log.tar.gz /home/shrish/airflow/dags/ETL_capitalized.txt