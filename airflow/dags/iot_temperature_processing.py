"""
DAG для обработки данных температуры IoT:
1. Фильтрация по out/in = 'In' (регистронезависимо)
2. Преобразование даты в формат yyyy-MM-dd (тип date)
3. Очистка температуры по 5-му и 95-му перцентилю
4. Расчет 5 самых жарких и холодных дней
"""
from datetime import datetime
import pandas as pd
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException

RAW_DATA_PATH = "/opt/airflow/data/IOT-temp.csv"
PROCESSED_PATH = "/opt/airflow/processed"
HOT_DAYS_PATH = f"{PROCESSED_PATH}/hottest_days.csv"
COLD_DAYS_PATH = f"{PROCESSED_PATH}/coldest_days.csv"

def load_and_filter_data(**context):
    """Загрузка данных и фильтрация по out/in = 'In' (регистронезависимо)"""
    df = pd.read_csv(RAW_DATA_PATH)
    
    print(f"✓ Загружено {len(df)} записей")
    print(f"  Колонки: {df.columns.tolist()}")
    print(f"  Уникальные значения 'out/in': {df['out/in'].unique()}")
    
    df_filtered = df[df['out/in'].str.lower() == 'in'].copy()
    
    if len(df_filtered) == 0:
        raise AirflowException(
            f"После фильтрации не осталось записей! "
            f"Доступные значения в колонке 'out/in': {df['out/in'].unique()}"
        )
    
    df_filtered['date'] = pd.to_datetime(
        df_filtered['noted_date'], 
        format='%d-%m-%Y %H:%M', 
        errors='coerce'
    ).dt.date
    
    df_filtered = df_filtered.dropna(subset=['date'])
    
    os.makedirs(PROCESSED_PATH, exist_ok=True)
    df_filtered.to_parquet(f"{PROCESSED_PATH}/filtered_data.parquet")
    
    context['ti'].xcom_push(key='filtered_rows', value=len(df_filtered))
    context['ti'].xcom_push(key='total_rows', value=len(df))
    
    print(f"✓ Отфильтровано {len(df_filtered)} записей из {len(df)} (out/in = 'In')")
    print(f"✓ Преобразовано дат: {len(df_filtered)}")

def clean_temperature(**context):
    """Очистка температуры по 5-му и 95-му перцентилю"""
    df = pd.read_parquet(f"{PROCESSED_PATH}/filtered_data.parquet")
    
    if len(df) == 0:
        raise AirflowException("DataFrame пустой после фильтрации!")
    
    p5 = df['temp'].quantile(0.05)
    p95 = df['temp'].quantile(0.95)
    
    df_clean = df[(df['temp'] >= p5) & (df['temp'] <= p95)].copy()
    outliers_removed = len(df) - len(df_clean)
    pct_removed = (outliers_removed / len(df)) * 100
    
    context['ti'].xcom_push(key='outliers_removed', value=outliers_removed)
    context['ti'].xcom_push(key='p5', value=float(p5))
    context['ti'].xcom_push(key='p95', value=float(p95))
    context['ti'].xcom_push(key='cleaned_rows', value=len(df_clean))
    
    df_clean.to_parquet(f"{PROCESSED_PATH}/cleaned_data.parquet")
    
    print(f"✓ Очистка завершена:")
    print(f"  - Диапазон температур: [{p5:.2f}°C, {p95:.2f}°C]")
    print(f"  - Удалено выбросов: {outliers_removed} из {len(df)} ({pct_removed:.2f}%)")
    print(f"  - Осталось записей: {len(df_clean)}")

def calculate_extreme_days(**context):
    """Расчет 5 самых жарких и холодных дней"""
    df = pd.read_parquet(f"{PROCESSED_PATH}/cleaned_data.parquet")
    
    if len(df) == 0:
        raise AirflowException("Нет данных для расчёта экстремальных дней!")
    
    daily_stats = df.groupby('date')['temp'].mean().reset_index()
    daily_stats.columns = ['date', 'avg_temp']
    
    hottest = daily_stats.nlargest(5, 'avg_temp')
    coldest = daily_stats.nsmallest(5, 'avg_temp')
    
    hottest.to_csv(HOT_DAYS_PATH, index=False)
    coldest.to_csv(COLD_DAYS_PATH, index=False)
    
    context['ti'].xcom_push(key='hottest_days', value=hottest.to_dict('records'))
    context['ti'].xcom_push(key='coldest_days', value=coldest.to_dict('records'))
    
    print("🔥 ТОП-5 самых жарких дней:")
    for i, row in hottest.iterrows():
        print(f"  {i+1}. {row['date']} — {row['avg_temp']:.2f}°C")
    
    print("\n❄️ ТОП-5 самых холодных дней:")
    for i, row in coldest.iterrows():
        print(f"  {i+1}. {row['date']} — {row['avg_temp']:.2f}°C")

def generate_report(**context):
    """Генерация итогового отчета"""
    ti = context['ti']
    
    total_rows = ti.xcom_pull(key='total_rows')
    filtered_rows = ti.xcom_pull(key='filtered_rows')
    outliers_removed = ti.xcom_pull(key='outliers_removed')
    p5 = ti.xcom_pull(key='p5')
    p95 = ti.xcom_pull(key='p95')
    cleaned_rows = ti.xcom_pull(key='cleaned_rows')
    hottest_days = ti.xcom_pull(key='hottest_days')
    coldest_days = ti.xcom_pull(key='coldest_days')
    
    report = f"""
==========================================
ОТЧЁТ ПО ОБРАБОТКЕ ДАННЫХ IOT-ТЕМПЕРАТУРЫ
==========================================
Дата обработки: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

Исходные данные:
  - Всего записей: {total_rows}
  - Записей после фильтрации (out/in = 'In'): {filtered_rows}
  - Процент отфильтрованных: {(filtered_rows/total_rows)*100:.2f}%

Очистка температуры:
  - 5-й перцентиль: {p5:.2f}°C
  - 95-й перцентиль: {p95:.2f}°C
  - Удалено выбросов: {outliers_removed}
  - Осталось записей после очистки: {cleaned_rows}

Экстремальные дни:
  Самые жаркие:
{chr(10).join([f'    {i+1}. {d["date"]} — {d["avg_temp"]:.2f}°C' for i, d in enumerate(hottest_days)])}

  Самые холодные:
{chr(10).join([f'    {i+1}. {d["date"]} — {d["avg_temp"]:.2f}°C' for i, d in enumerate(coldest_days)])}

Результаты сохранены в:
  - {HOT_DAYS_PATH}
  - {COLD_DAYS_PATH}
==========================================
"""
    print(report)
    
    with open(f"{PROCESSED_PATH}/processing_report.txt", "w") as f:
        f.write(report)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
}

with DAG(
    'iot_temperature_processing',
    default_args=default_args,
    description='Обработка данных температуры IoT устройств',
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['iot', 'temperature', 'data-cleaning'],
) as dag:

    t1 = PythonOperator(
        task_id='load_and_filter_data',
        python_callable=load_and_filter_data,
        provide_context=True,
    )

    t2 = PythonOperator(
        task_id='clean_temperature',
        python_callable=clean_temperature,
        provide_context=True,
    )

    t3 = PythonOperator(
        task_id='calculate_extreme_days',
        python_callable=calculate_extreme_days,
        provide_context=True,
    )

    t4 = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report,
        provide_context=True,
    )

    t1 >> t2 >> t3 >> t4