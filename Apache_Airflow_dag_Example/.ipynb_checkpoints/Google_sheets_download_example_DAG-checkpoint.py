### --- Импорт библиотек ---

# === Стандартная библиотека ===
import requests

# === Работа с временем и датами ===
import pendulum

# === Airflow ===
from airflow import DAG
from airflow.decorators import dag, task
from airflow.hooks.base_hook import BaseHook

### --- --- ###

# Владелец
default_args = {
    'owner': 'username_example',
    'retries': 2,
    'retry_delay': pendulum.duration(minutes=5),
}


# Параметры запуска DAG'а
@dag(
    schedule='0 8 * * 3',  # каждую среду в 8 утра
    start_date=pendulum.datetime(2025, 1, 1, tz="Europe/Moscow"),
    catchup=False,
    tags=["Tag_Example"], # По данному тегу можно найти в списке дагов
    default_args=default_args
)
def m_pid_sub_base_list_updater():
    import numpy as np
    import pandas as pd
    import clickhouse_connect

#######################################################################################
################  Step 1 ####################################################
######################################################################################    
    @task #### Настраиваем подключение с вал
    def df_from_val(query, base="val"):     
        conn_val = BaseHook.get_connection('username_val_conn') # # Пример соединения
        client = clickhouse_connect.get_client(
            host=conn_val.host,
            database=base,
            username=conn_val.login,
            password=conn_val.password
        )
        pd.set_option('display.float_format', lambda x: '%.10f' % x)        
        df = client.query_df(query)
        
        return df

    @task ### Настраиваем подключение с м_партнерс
    def df_from_m_partner(query, base="m_partner"):     
        conn_val = BaseHook.get_connection('username_m_partner') # # Пример соединения
        client = clickhouse_connect.get_client(
            host=conn_val.host,
            database=base,
            username=conn_val.login,
            password=conn_val.password
        )
        pd.set_option('display.float_format', lambda x: '%.10f' % x)        
        df = client.query_df(query)
        
        return df

    @task    
    def process_m_info(df):
        # Преобразование списка в строку значений, разделенных запятыми
        sub1_data = df.loc[(df['sub_type'] == 'sub1')].reset_index(drop=True)
        sub2_data = df.loc[(df['sub_type'] == 'sub2')].reset_index(drop=True)
        sub3_data = df.loc[(df['sub_type'] == 'sub3')].reset_index(drop=True)
    
        correct_partner_id_sub1_data = list(sub1_data['partner_id'].unique())
        correct_partner_id_sub2_data = list(sub2_data['partner_id'].unique())
        correct_partner_id_sub3_data = list(sub3_data['partner_id'].unique())
    
        str_sub1_data = ', '.join(map(str, correct_partner_id_sub1_data))
        str_sub2_data = ', '.join(map(str, correct_partner_id_sub2_data))
        str_sub3_data = ', '.join(map(str, correct_partner_id_sub3_data))
    
        return {
            "sub1": str_sub1_data,
            "sub2": str_sub2_data,
            "sub3": str_sub3_data
        }

    @task # Задача для формирования запроса с данными 1
    def query_bi_users_maker(data):
        query = f"""
        SELECT
                    partner_id_final AS partner_id,
                    CASE 
                            WHEN project_id = 1 THEN 'Mos' 
                            WHEN project_id = 2 THEN 'Be'
                            WHEN project_id = 4 THEN 'Ba'
                        ELSE 'Vi' 
                    END AS project_id,
                    sub1 AS sub_id,
                    'sub1' AS sub_type,
                    MAX(reg_date) AS max_reg_date,
                    MAX(fd_date) AS max_fd_date
            FROM bi.users
                WHERE partner_id_final IN ({data["sub1"]}) 
                GROUP BY partner_id_final, 
                         project_id, 
                         sub1
        
        UNION ALL
        
        SELECT
                    partner_id_final AS partner_id,
                    CASE 
                            WHEN project_id = 1 THEN 'Mos' 
                            WHEN project_id = 2 THEN 'Be'
                            WHEN project_id = 4 THEN 'Ba'
                        ELSE 'Vi' 
                    END AS project_id,
                    sub2 AS sub_id,
                    'sub2' AS sub_type,
                    MAX(reg_date) AS max_reg_date,
                    MAX(fd_date) AS max_fd_date
            FROM bi.users
                WHERE partner_id_final IN ({data["sub2"]}) 
                GROUP BY partner_id_final, 
                         project_id, 
                         sub2
        
        UNION ALL
        
        SELECT
                    partner_id_final AS partner_id,
                    CASE 
                            WHEN project_id = 1 THEN 'Mos' 
                            WHEN project_id = 2 THEN 'Be'
                            WHEN project_id = 4 THEN 'Ba'
                        ELSE 'Vi' 
                    END AS project_id,
                    sub3 AS sub_id,
                    'sub3' AS sub_type,
                    MAX(reg_date) AS max_reg_date,
                    MAX(fd_date) AS max_fd_date
            FROM bi.users
                WHERE partner_id_final IN ({data["sub3"]}) 
                GROUP BY partner_id_final, 
                         project_id, 
                         sub3
        """
        
        return query

    @task
    def time_str_maker():
        from datetime import datetime, timedelta
        # Получаем сегодняшнюю дату
        today = datetime.today()        
        # Определяем последнее воскресенье
        days_since_sunday = (today.weekday() + 1) % 7  # weekday() возвращает 0 для понедельника, 6 для воскресенья        
        # Вычисляем дату прошлого воскресенья
        last_sunday = today - timedelta(days=days_since_sunday + 7)
        # Форматируем в нужный формат
        sunday_str = last_sunday.strftime("%Y-%m-%d")
        
        return sunday_str

    @task # Задача для формирования запроса с данными 2
    def query_m_partner_maker(data, last_sunday_str):
        query = f"""
        
        SELECT 
                    partner_id ,
                    CASE 
                            WHEN project_id = 1 THEN 'Mos' 
                            WHEN project_id = 2 THEN 'Be'
                            WHEN project_id = 4 THEN 'Ba'
                        ELSE 'Vi' 
                    END AS project_id,
                    sub1 AS sub_id,
                    'sub1' AS sub_type,
                    min(registration_date) AS max_reg_date,
                    toDateTime('1970-01-01 03:00:00') AS max_fd_date
            FROM m_partner.statistics
                WHERE partner_id IN ({data["sub1"]}) 
                GROUP BY partner_id, 
                         project_id, 
                         sub1
                HAVING MIN(registration_date) > '{last_sunday_str}'
                
                UNION ALL
                
        SELECT 
                    partner_id ,
                    CASE 
                            WHEN project_id = 1 THEN 'Mos' 
                            WHEN project_id = 2 THEN 'Be'
                            WHEN project_id = 4 THEN 'Ba'
                        ELSE 'Vi' 
                    END AS project_id,
                    sub2 AS sub_id,
                    'sub2' AS sub_type,
                    min(registration_date) AS max_reg_date,
                    toDateTime('1970-01-01 03:00:00') AS max_fd_date
            FROM m_partner.statistics
                WHERE partner_id IN ({data["sub2"]}) 
                GROUP BY partner_id, 
                         project_id, 
                         sub2
                HAVING MIN(registration_date) > '{last_sunday_str}'
                
                UNION ALL
                
        SELECT 
                    partner_id ,
                    CASE 
                            WHEN project_id = 1 THEN 'Mos' 
                            WHEN project_id = 2 THEN 'Be'
                            WHEN project_id = 4 THEN 'Ba'
                        ELSE 'Vi' 
                    END AS project_id,
                    sub3 AS sub_id,
                    'sub3' AS sub_type,
                    min(registration_date) AS max_reg_date,
                    toDateTime('1970-01-01 03:00:00') AS max_fd_date
            FROM m_partner.statistics
                WHERE partner_id IN ({data["sub3"]}) 
                GROUP BY partner_id, 
                         project_id, 
                         sub3
                HAVING MIN(registration_date) > '{last_sunday_str}'
        """
        
        return query

    @task ### для причесывания данных
    def valid_data_maker(data_1, data_2):
        # Преобразуем колонку в формат даты
        data_1[['max_reg_date', 
                'max_fd_date']] = data_1[['max_reg_date', 'max_fd_date']].apply(pd.to_datetime)
        
        data_2[['max_reg_date', 
                'max_fd_date']] = data_2[['max_reg_date', 'max_fd_date']].apply(pd.to_datetime)
        
        data_1 = data_1.astype({
                                'partner_id': 'int64',
                                'project_id': 'str',
                                'sub_id': 'str',
                                'sub_type': 'str'
                            })
        data_2 = data_2.astype({
                                'partner_id': 'int64',
                                'project_id': 'str',
                                'sub_id': 'str',
                                'sub_type': 'str'
                            })
        final_sub_list_data = pd.concat([data_1, data_1], ignore_index=True)
        
        return final_sub_list_data

#######################################################################################
################  Step 2 ####################################################
###################################################################################### 
    @task # Google
    def simple_excel_google_sheets(api_google_sn, sheet_url, dataframes, delegated_email, users):
        import gspread
        import pandas as pd
        from google.oauth2.service_account import Credentials
        import os    
        print("Current working directory:", os.getcwd())
        
        # Настраиваем доступ к Google Sheets через сервисный аккаунт с делегированием
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        credentials = Credentials.from_service_account_file(api_google_sn, scopes=scopes)
        credentials = credentials.with_subject(delegated_email)
        client = gspread.authorize(credentials) 
        
        try:
            # Открываем существующий файл по ссылке
            spreadsheet = client.open_by_url(sheet_url)
            print(f'Файл по ссылке "{sheet_url}" найден. Перезаписываем данные.')
        except Exception as e:
            print(f'Не удалось открыть файл по ссылке: {e}')
            
            return
        
        # Обрабатываем каждый DataFrame и соответствующий лист
        for name, df in dataframes.items():
            # Преобразуем даты в строки
            for col in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    df[col] = df[col].astype(str)
    
            try:
                worksheet = spreadsheet.worksheet(name)
                worksheet.clear()
                print(f'Лист "{name}" найден. Перезаписываем данные.')
            except gspread.WorksheetNotFound:
                worksheet = spreadsheet.add_worksheet(title=name, rows=df.shape[0] + 1, cols=df.shape[1])
                print(f'Создан новый лист: {name}')    
            # Подготовка и запись данных одной пачкой
            data = [df.columns.tolist()] + df.astype(str).values.tolist()
            worksheet.update("A1", data)
            print(f'Лист "{name}" обновлён. Всего строк (включая заголовки): {len(data)}')
    
        # Шарим доступ
        if users:
            for user in users:
                #spreadsheet.share(user, perm_type='user', role='writer')
                print(f'Доступ открыт для {user}')
        else:
            print('Список пользователей пуст, доступ не предоставлен.')
    
        print(f'Файл успешно записан.')

     ###   
    @task # сообщение отбивка в Телеграмм бота
    def send_message_tg(bot_conn, message ):
        #message = "🔊Test message! ✅"  # Здесь можно динамически формировать сообщение
        
        telegram_conn = BaseHook.get_connection(bot_conn)  # Имя подключения
        bot_token = telegram_conn.password  # Токен бота
        chat_id = telegram_conn.host  # chat_id, который хранится в host
        print(f"Using bot token: {bot_token}")
        print(f"Using chat id: {chat_id}")
        
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        data = {
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "HTML"
        }
        response = requests.post(url, data=data)
        print(f'Sending message: {message}')
        print(f'Response: {response.status_code}, {response.text}')
        if response.status_code != 200:
            print(f"Failed to send message: {response.text}")
            
            
#######################################################################################
################  Step 3 ####################################################
###################################################################################### 
    # Шаги: извлекаем данные, обрабатываем их и строим запрос
    # Основной процесс
    query_m_info = f"""
    SELECT site, partner_id, sub_type
    FROM reports.m_info
    WHERE business_unit ILIKE '%acquis%' AND sub_type != ''
    """
    # инфа для гугла
    api_google_sn = r'/opt/airflow/dags/advivambcash-591ssda91a.json'
    delegated_email = 'd.username_example@ars.cloud'
    medbuy_ratings_file_name = 'https://docs.google.com/spreadsheets/d/1a665fIJUj2kytlf2YmfxrBjgKlLp'
    users = ["d.username_example@ars.cloud"]
    
    bot = send_message_tg(bot_conn = "m_alerts_bot", message = "🔊<b>Файл:<u>Расходы на партнеров</u></b>\n\n👨‍💻Начинаю обновление листа базы")

    
    # Шаги: извлекаем данные, обрабатываем их и строим запрос
    m_info__ = df_from_val(query = query_m_info, base = "reports")
    partner_id_strings = process_m_info(m_info__)
    # Используем результат для формирования запроса
    query_bi_users = query_bi_users_maker(partner_id_strings)
    # Подгружаем данные из юзерс
    val_partner_data = df_from_val(query = query_bi_users, base = "bi")
    # вычисляем дату
    last_sunday_str = time_str_maker()
    query_m_partner = query_m_partner_maker(data = partner_id_strings, last_sunday_str = last_sunday_str)
    m_partner_data = df_from_m_partner( query = query_m_partner, base = "m_partner" )

####### FINAL
    final__data = valid_data_maker(data_1 = val_partner_data, data_2 = m_partner_data)
    dataframes = {'base':final__data}

    data_to_google = simple_excel_google_sheets(api_google_sn, medbuy_ratings_file_name, dataframes, delegated_email, users)

    bot = send_message_tg(bot_conn = "m_alerts_bot", message = "🔊<b>Файл:<u>Расходы на партнеров</u></b>\n\n👨‍💻Данные на листе base обновленны! ✅")

### Собираем даг:
    (
        m_info__ >> 
        partner_id_strings >> 
        query_bi_users >> 
        val_partner_data >> 
        last_sunday_str >> 
        query_m_partner >> 
        m_partner_data >> 
        final__data >>
        data_to_google >>
        bot
    )

m_pid_sub_base_list_updater()