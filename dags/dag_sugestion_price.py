from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 16),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'xgboost_prediction_dag',
    default_args=default_args,
    schedule_interval='0 8,12,15,17 * * 1-5',  
    catchup=False,
)

def run_xgboost_prediction():
    import warnings
    warnings.simplefilter(action='ignore', category=FutureWarning)
    import pandas as pd
    from sklearn.model_selection import train_test_split
    from xgboost import XGBRegressor
    from sklearn.preprocessing import OneHotEncoder
    from scipy.stats import norm
    from sklearn.model_selection import GridSearchCV
    
    def calculate_probability_of_sale(row):
        price_difference = row['sugestao_preco'] - row['precoconcorrente']
        std_deviation = row['preco_real'] - row['sugestao_preco']
        if std_deviation == 0:
            return 'N/A'
        z_score = price_difference / std_deviation
        probability = 1 - norm.cdf(z_score)  
        return '{:.2%}'.format(probability)

    def train_and_predict(csv_file_path):
        dados = pd.read_csv(csv_file_path, sep=";", encoding="latin-1")
        dados.fillna(0, inplace=True)
        dados[['preco', 'margem', 'precoconcorrente']] = dados[['preco', 'margem', 'precoconcorrente']].applymap(
            lambda k: float(str(k).replace(",", "").replace(".", "")))

       
    csv_file_path = r'D:\dev0608\google_teste.csv'
    results_dataframe = train_and_predict(csv_file_path)

    print(results_dataframe)

run_xgboost_prediction_task = PythonOperator(
    task_id='run_xgboost_prediction_task',
    python_callable=run_xgboost_prediction,
    dag=dag,
)


als_execution_task >> run_xgboost_prediction_task