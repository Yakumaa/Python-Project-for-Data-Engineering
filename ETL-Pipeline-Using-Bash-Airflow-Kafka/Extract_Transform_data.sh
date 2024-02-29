tar -xzf /home/shrish/airflow/dags/tolldata.tgz

cut -f1-4 -d"#" /home/shrish/airflow/dags/vehicle-data.csv > /home/shrish/airflow/dags/csv_data.csv

cut -f5,7 -d"#" /home/shrish/airflow/dags/tollplaza-data.tsv > /home/shrish/airflow/dags/tsv_data.csv

awk 'NF{print $(NF-1),$NF}' OFS="\t" /home/shrish/airflow/dags/payment-data.txt > /home/shrish/airflow/dags/fixed_width_data.csv

paste /home/shrish/airflow/dags/csv_data.csv /home/shrish/airflow/dags/tsv_data.csv \
       /home/shrish/airflow/dags/fixed_width_data.csv > /home/shrish/airflow/dags/extracted_data.csv

tr "[a-z]" "[A-Z]" < /home/shrish/airflow/dags/extracted_data.csv > /home/shrish/airflow/dags/transformed_data.csv


