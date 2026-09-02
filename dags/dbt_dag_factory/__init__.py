"""DAG factory: đọc airflow_config/*.yml và sinh DAG dbt cho repo này.

Module này KHÔNG được dag-processor parse trực tiếp (xem dags/.airflowignore).
Điểm vào duy nhất là dags/dag_builder.py.
"""
