from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# 앞선 02는 date_now = datetime.now를 사용했기 때문에
# 언제 실행해도 우리가 실행한 시간 기준 실행
# airflow batck성으로 특정 시간대로 실행하는 도구, now 등을 잘 쓰지 않음. 의도한 시간, 날짜 주입해서 사용
# airflow로 과거 날짜로 실행해야 되는 경우도 존재. 과거 데이터 마이그레이션
# 코드 상에서 now, sql 상에서도 current_date 사용하지않고, airflow에서 실행하기로 했던 시간 넣어야함
# execution_date, logical_date
# 멱등성 : 연산을 여러 번 적용해도 결과가 달라지지 않는 성질

default_args = {
  "owner": "oh",
  "depends_on_past": False, # 이전 DAG의 Task 성공 여부에 따라서 현재 Task를 실행할지 말지가 결정. False는 고거 Task의 성공 여부와 상관없이 실행
  "start_date": datetime(2024, 1, 1),
  "end_date": datetime(2024, 1, 4)
}

def print_current_date_with_context(*args, **kwargs):
  """
  kwargs: {'conf': <airflow.configuration.AirflowConfigParser object at 0x7f34bed45880>, 
  'dag': <DAG: python_dag_with_context>, 
  'dag_run': <DagRun python_dag_with_context @ 2024-01-02 00:30:00+00:00: scheduled__2024-01-02T00:30:00+00:00, state:running, queued_at: 2026-01-13 10:18:33.523670+00:00. externally triggered: False>, 
  'data_interval_end': DateTime(2024, 1, 3, 0, 30, 0, tzinfo=Timezone('UTC')), 
  'data_interval_start': DateTime(2024, 1, 2, 0, 30, 0, tzinfo=Timezone('UTC')), 
  'ds': '2024-01-02', 
  'ds_nodash': '20240102', 
  'execution_date': <Proxy at 0x7f34b9dcff00 with factory functools.partial(<function lazy_mapping_from_context.<locals>._deprecated_proxy_factory at 0x7f34b9fc1440>, 
  'execution_date', DateTime(2024, 1, 2, 0, 30, 0, tzinfo=Timezone('UTC')))>, 'expanded_ti_count': None, 'inlets': [], 'logical_date': DateTime(2024, 1, 2, 0, 30, 0, tzinfo=Timezone('UTC')), 'macros': <module 'airflow.macros' from '/home/dhtnqls0103/boostcamp-ai-tech/.venv/lib/python3.12/site-packages/airflow/macros/__init__.py'>, 
  'map_index_template': None, 'next_ds': <Proxy at 0x7f34b9f72940 with factory functools.partial(<function lazy_mapping_from_context.<locals>._deprecated_proxy_factory at 0x7f34b9fc1440>, 
  'next_ds', '2024-01-03')>, 
  'next_ds_nodash': <Proxy at 0x7f34b9ff3b40 with factory functools.partial(<function lazy_mapping_from_context.<locals>._deprecated_proxy_factory at 0x7f34b9fc1440>, 
  'next_ds_nodash', '20240103')>, 
  'next_execution_date': <Proxy at 0x7f34b9eccbc0 with factory functools.partial(<function lazy_mapping_from_context.<locals>._deprecated_proxy_factory at 0x7f34b9fc1440>, 
  'next_execution_date', DateTime(2024, 1, 3, 0, 30, 0, tzinfo=Timezone('UTC')))>, 
  'outlets': [], 'params': {}, 
  'prev_data_interval_start_success': DateTime(2024, 1, 1, 0, 30, 0, tzinfo=Timezone('UTC')), 
  'prev_data_interval_end_success': DateTime(2024, 1, 2, 0, 30, 0, tzinfo=Timezone('UTC')), 
  'prev_ds': <Proxy at 0x7f34b9fb0b00 with factory functools.partial(<function lazy_mapping_from_context.<locals>._deprecated_proxy_factory at 0x7f34b9fc1440>, 
  'prev_ds', '2024-01-01')>, 
  'prev_ds_nodash': <Proxy at 0x7f34b9fb3b80 with factory functools.partial(<function lazy_mapping_from_context.<locals>._deprecated_proxy_factory at 0x7f34b9fc1440>, 
  'prev_ds_nodash', '20240101')>, 
  'prev_execution_date': <Proxy at 0x7f34b9dd9180 with factory functools.partial(<function lazy_mapping_from_context.<locals>._deprecated_proxy_factory at 0x7f34b9fc1440>, 
  'prev_execution_date', DateTime(2024, 1, 1, 0, 30, 0, tzinfo=Timezone('UTC')))>, 
  'prev_execution_date_success': <Proxy at 0x7f34b9dd97c0 with factory functools.partial(<function lazy_mapping_from_context.<locals>._deprecated_proxy_factory at 0x7f34b9fc1440>, 
  'prev_execution_date_success', DateTime(2024, 1, 1, 0, 30, 0, tzinfo=Timezone('UTC')))>, 
  'prev_start_date_success': DateTime(2026, 1, 13, 10, 18, 28, 455677, tzinfo=Timezone('UTC')), 
  'prev_end_date_success': DateTime(2026, 1, 13, 10, 18, 33, 597712, tzinfo=Timezone('UTC')), 
  'run_id': 'scheduled__2024-01-02T00:30:00+00:00', 'task': <Task(PythonOperator): print_current_date_with_context>, 'task_instance': <TaskInstance: python_dag_with_context.print_current_date_with_context scheduled__2024-01-02T00:30:00+00:00 [running]>, 'task_instance_key_str': 'python_dag_with_context__print_current_date_with_context__20240102', 'test_mode': False, 'ti': <TaskInstance: python_dag_with_context.print_current_date_with_context scheduled__2024-01-02T00:30:00+00:00 [running]>, 'tomorrow_ds': <Proxy at 0x7f34b9dd9900 with factory functools.partial(<function lazy_mapping_from_context.<locals>._deprecated_proxy_factory at 0x7f34b9fc1440>, 'tomorrow_ds', '2024-01-03')>, 'tomorrow_ds_nodash': <Proxy at 0x7f34b9dda7c0 with factory functools.partial(<function lazy_mapping_from_context.<locals>._deprecated_proxy_factory at 0x7f34b9fc1440>, 'tomorrow_ds_nodash', '20240103')>, 'triggering_dataset_events': <Proxy at 0x7f34b9f754c0 with factory <function _get_template_context.<locals>.get_triggering_events at 0x7f34b9f61bc0>>, 'ts': '2024-01-02T00:30:00+00:00', 'ts_nodash': '20240102T003000', 'ts_nodash_with_tz': '20240102T003000+0000', 'var': {'json': None, 'value': None}, 'conn': None, 'yesterday_ds': <Proxy at 0x7f34b9dda800 with factory functools.partial(<function lazy_mapping_from_context.<locals>._deprecated_proxy_factory at 0x7f34b9fc1440>, 'yesterday_ds', '2024-01-01')>, 'yesterday_ds_nodash': <Proxy at 0x7f34b9dda840 with factory functools.partial(<function lazy_mapping_from_context.<locals>._deprecated_proxy_factory at 0x7f34b9fc1440>, 'yesterday_ds_nodash', '20240101')>, 'templates_dict': None}
[2026-01-13, 10:18:36 UTC] {python.py:237} INFO - Done. Returned value was: None
  """
  print(f"kwargs: {kwargs}")
  execution_date = kwargs["ds"]
  execution_date_nodash = kwargs["ds_nodash"]
  print(f"execution_date_nodash : {execution_date_nodash}")
  execution_date = datetime.strptime(execution_date, "%Y-%m-%d").date()
  date_kor = ["월","화","수","목","금","토","일"]
  datetime_weeknum = execution_date.weekday()
  print(f"{execution_date}는 {date_kor[datetime_weeknum]}요일입니다")

  # execution_date

with DAG(
  dag_id="python_dag_with_context",
  default_args=default_args,
  schedule_interval="30 0 * * *", # UTC 시간 기준으로 매일 0시 30분, 한국 시간 9시 30분
  tags=['my_dags'],
  catchup=True
) as dag:
  
  python_task = PythonOperator(
    task_id="print_current_date_with_context",
    python_callable=print_current_date_with_context
  )