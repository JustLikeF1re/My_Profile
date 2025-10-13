### --- Импорт библиотек ---

# === Стандартная библиотека ===
import os
import asyncio
import logging
from datetime import datetime
from io import StringIO

# === Аналитика и обработка данных ===
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# === Работа с изображениями ===
from PIL import Image

# === Телеграм-бот (Aiogram) ===
from aiogram import Bot, Dispatcher, types
from aiogram.filters import CommandStart, Command
from aiogram.types import Message

# === Работа с базами данных ===
import clickhouse_connect

# === Пользовательские настройки и конфигурация ===
from Config_file import (
    TOKEN,
    ALLOWED_USERS,
    creds_user,
    creds_password,
    file_path
)


### --- --- ###

bot = Bot(token=TOKEN)
dp = Dispatcher()

# Подключение к ClickHouse

client = clickhouse_connect.get_client(host='ClickHouse.base.example', port=8123, username=creds_user,
                                       password=creds_password, database='reports')

# Запрос к таблице
query = """
            WITH 
                (SELECT max(added_dt) AS max_added_dt 
                    FROM reports.me_log_weekly_data_fact 
                        WHERE lower(`comment`) NOT LIKE '%закрыт%') 
                    AS max_added_dt
            SELECT *
                FROM reports.me_log_weekly_data_fact
                    WHERE added_dt = max_added_dt
                        AND lower(`comment`) NOT LIKE '%закрыт%'
"""

# Выполнение запроса и получение данных в виде DataFrame
df = client.query_df(query)
# Замена пропусков в определенных колонках
df['buying_team'] = df['buying_team'].fillna('!Empty')
columns_to_replace = ['sub_id', 'sub_type', 'comment']
df[columns_to_replace] = df[columns_to_replace].fillna("")
# список партнеров продукта
query_2 = """
            SELECT partner_id , business_unit 
                FROM reports.me_info
"""
me_info = client.query_df(query_2)
me_info['partner_id'] = me_info['partner_id'].astype(int)
df['partner_id'] = df['partner_id'].astype(int)
df['yyyyww'] = df['yyyyww'].astype(int)
for_count_yyyyww_data = df.copy()
### фильтр когорт
now = pd.Timestamp.now(tz='Europe/Moscow')
today = now.tz_localize(None).normalize()

end = now.date() - pd.tseries.offsets.Week(weekday=6)
end_str = end.strftime('%Y-%m-%d')
end_yyyyww = end.isocalendar().year * 100 + end.isocalendar().week
end_yyyymm = end.year * 100 + end.month

start = end - pd.Timedelta(days=7*7-1) #18
start_str = start.strftime('%Y-%m-%d')
start_yyyyww = start.isocalendar().year * 100 + start.isocalendar().week
start_yyyymm = start.year * 100 + start.month
## конец фильтра когорт
df = df[df['yyyyww'] >= start_yyyyww].reset_index(drop=True)

assert df.isna().sum().sum() == 0  # Проверка на наличие пропусков в таблице
assert me_info.isna().sum().sum() == 0  # Проверка на наличие пропусков в таблице

# Формирование выборок по командам
Fmedia_me_info = me_info[me_info['business_unit'] == 'Fmedia'].reset_index(drop=True)
Acti_me_info = me_info[me_info['business_unit'] == 'Acti'].reset_index(drop=True)

### вычисление актуальной версии модели в бд
datetime_str = df['added_dt'].max()
datetime_obj = datetime.strptime(datetime_str, '%Y.%m.%d_%H:%M:%S')
date_str = datetime_obj.strftime('%Y.%m.%d')

# Удаление лишних колонок
df = df.drop(columns=['rake_today', 'rake_cum_of_payback_month'])

for_answer_data_subs = df[['project',
                           'type',
                           'yyyyww',
                           'geo',
                           'partner_id',
                           'sub_type',
                           'sub_id',
                           'source',
                           'reg',
                           'fd',
                           'rd_count',
                           'one_day_players',
                           'fact_fee',
                           'ggr',
                           'turnover_margin',
                           'margin_includes_organics',
                           'margin_includes_organics_and_costs',
                           'payback_forecast_months']].copy()
# Функция для безопасного деления
def safe_divide(fee, fd):
    try:
        return fee / fd if fd != 0 else 0
    except ZeroDivisionError:
        return 0
# Расчет необходимых метрик    
for_answer_data_subs['доля однодневок'] = (
                                            for_answer_data_subs.
                                            apply(lambda row: 
                                                  safe_divide(row['one_day_players'], 
                                                                          row['fd']), 
                                                  axis=1)
                                            )
for_answer_data_subs['доля однодневок %'] = (for_answer_data_subs['доля однодневок'] * 100).round(2)
for_answer_data_subs['Средние издержки'] = (
                                            for_answer_data_subs.
                                            apply(lambda row: 
                                                  safe_divide(row['margin_includes_organics_and_costs'], 
                                                                          row['margin_includes_organics']), 
                                                  axis=1)
                                        )
for_answer_data_subs['Средние издержки_%'] = (for_answer_data_subs['Средние издержки'] * 100).round(2)
for_answer_data_subs['Средние издержки %'] = (100 - for_answer_data_subs['Средние издержки_%'])
for_answer_data_subs['Маржа от органики $'] = (
                                                for_answer_data_subs['margin_includes_organics'] 
                                                - 
                                                for_answer_data_subs['turnover_margin']
                                            )
for_answer_data_subs = for_answer_data_subs[[
                                                'project',
                                                'type',
                                                'yyyyww',
                                                'geo',
                                                'partner_id',
                                                'sub_type',
                                                'sub_id',
                                                'source',
                                                'reg',
                                                'fd',
                                                'rd_count',
                                                'доля однодневок %',
                                                'fact_fee',
                                                'ggr',
                                                'turnover_margin',
                                                'Маржа от органики $',
                                                'Средние издержки %',
                                                'payback_forecast_months'
                                            ]]
# Переименовываем колонки
for_answer_data_subs = (
                        for_answer_data_subs.
                            rename(
                                    columns={'project': 'Проект',
                                            'type': 'Тип',
                                            'yyyyww': 'Когорта',
                                            'partner_id': 'Веб',
                                            'geo': 'Гео',
                                            'reg': 'Регистрации',
                                            'fd': 'Количество FD',
                                            'rd_count': 'Количество RD',
                                            'fact_fee': 'Расходы на трафик $',
                                            'ggr': 'GGR',
                                            'доля однодневок %': 'Доля однодневок %',
                                            'source': 'Источник',
                                            'turnover_margin': 'Маржа без органики $',
                                            'payback_forecast_months': 'Прогноз окупаемости мес.'})
                                   )
######################################## Групируем по пидам #################################################
# Колонки для вычислений группировки по пидам
need_columns_list = (
                        ['reg',
                         'fd',
                         'rd_count',
                         'one_day_players',
                         'fact_fee',
                         'ggr',
                         'turnover_margin',
                         'margin_includes_organics',
                         'margin_includes_organics_and_costs'] 
                         + 
                         list(df.filter(regex=r'^rake').columns)
                    )
data_pid_groupd = df.groupby(['project', 
                              'type', 
                              'yyyyww', 
                              'geo', 
                              'partner_id'])[need_columns_list].sum().reset_index()
need_columns_list_for_funx = ['fact_fee'] + list(data_pid_groupd.filter(regex=r'^rake\d+$').columns)

# Функция для вычисления окупаемости
def calculate_payback_row(row, df):
    fee_sum = row['fact_fee']  # Сумма fee для данной строки
    cumulative_sum = 0  # Начальная сумма rake, которая будет накапливаться
    rake_columns = list(df.filter(regex=r'^rake\d+$').columns)  # Поиск колонок, начинающихся с 'rake'

    # Проход по всем rake колонкам
    for column in rake_columns:
        cumulative_sum += row[column]  # Добавление значения текущей rake колонки к cumulative_sum
        if cumulative_sum > fee_sum:  # Проверка, если накопленная сумма превышает fee_sum
            # Извлечение номера из названия колонки
            rake_number = int(column.replace('rake', ''))
            return rake_number  # Возвращаем номер колонки
    return 99999  # Если ни одна из rake колонок не превысила fee_sum, возвращаем это сообщение

data_pid_groupd['payback_forecast_weaks'] = (
                                            data_pid_groupd.
                                            apply(
                                                lambda row: 
                                                calculate_payback_row(row, 
                                                                      data_pid_groupd), 
                                                axis=1)
                                            )

data_pid_groupd['payback_forecast_months'] = np.ceil((data_pid_groupd['payback_forecast_weaks'] * 7) / 30.5)
data_pid_groupd['payback_forecast_months'] = data_pid_groupd['payback_forecast_months'].astype(int)
data_pid_groupd['payback_forecast_months'] = data_pid_groupd['payback_forecast_months'].replace(22951, 999999)

data_pid_groupd['доля однодневок'] = (
                                        data_pid_groupd.
                                        apply(lambda row:
                                              safe_divide(row['one_day_players'], 
                                                          row['fd']),
                                              axis=1)
                                     )
data_pid_groupd['доля однодневок %'] = (data_pid_groupd['доля однодневок'] * 100).round(2)

data_pid_groupd['Средние издержки'] = (
                                        data_pid_groupd.
                                        apply(lambda row:
                                              safe_divide(row['margin_includes_organics_and_costs'], 
                                                          row['margin_includes_organics']), 
                                              axis=1)
                                    )
data_pid_groupd['Средние издержки_%'] = (data_pid_groupd['Средние издержки'] * 100).round(2)
data_pid_groupd['Средние издержки %'] = (100 - data_pid_groupd['Средние издержки_%'])

data_pid_groupd['Маржа от органики $'] = (
                                            data_pid_groupd['margin_includes_organics'] 
                                            - 
                                            data_pid_groupd['turnover_margin']
                                         )
data_pid_groupd = data_pid_groupd[[
                                    'project',
                                    'type',
                                    'yyyyww',
                                    'geo',
                                    'partner_id',
                                    'reg',
                                    'fd',
                                    'rd_count',
                                    'доля однодневок %',
                                    'fact_fee',
                                    'ggr',
                                    'turnover_margin',
                                    'Маржа от органики $',
                                    'Средние издержки %',
                                    'payback_forecast_months'
                                ]]
# Переименовываем колонки
data_pid_groupd = data_pid_groupd.rename(columns={'project': 'Проект',
                                                  'type': 'Тип',
                                                  'yyyyww': 'Когорта',
                                                  'partner_id': 'Веб',
                                                  'geo': 'Гео',
                                                  'reg': 'Регистрации',
                                                  'fd': 'Количество FD',
                                                  'rd_count': 'Количество RD',
                                                  'ggr': 'GGR',
                                                  'доля однодневок %': 'Доля однодневок %',
                                                  'fact_fee': 'Расходы на трафик $',
                                                  'turnover_margin': 'Маржа без органики $',
                                                  'payback_forecast_months': 'Прогноз окупаемости мес.'})
assert data_pid_groupd.isna().sum().sum() == 0  # Проверка на наличие пропусков в итоговой таблице пидов
assert for_answer_data_subs.isna().sum().sum() == 0  # Проверка на наличие пропусков в итоговой таблице сабов

# Причесываем данные
for_answer_data_subs.loc[for_answer_data_subs['Прогноз окупаемости мес.'] == 999999, 
                         'Прогноз окупаемости мес.'] = 'никогда'

data_pid_groupd.loc[data_pid_groupd['Прогноз окупаемости мес.'] == 999999, 
                    'Прогноз окупаемости мес.'] = 'никогда'

data_pid_groupd[['Доля однодневок %', 
                 'Расходы на трафик $', 
                 'GGR', 
                 'Маржа без органики $', 
                 'Маржа от органики $', 
                 'Средние издержки %']] = data_pid_groupd[['Доля однодневок %', 
                                                           'Расходы на трафик $', 
                                                           'GGR', 
                                                           'Маржа без органики $', 
                                                           'Маржа от органики $', 
                                                           'Средние издержки %']].round(0)
data_pid_groupd[['Доля однодневок %', 
                 'Расходы на трафик $', 
                 'GGR', 
                 'Маржа без органики $', 
                 'Маржа от органики $', 
                 'Средние издержки %']] = data_pid_groupd[['Доля однодневок %', 
                                                           'Расходы на трафик $', 
                                                           'GGR', 
                                                           'Маржа без органики $', 
                                                           'Маржа от органики $', 
                                                           'Средние издержки %']].astype(int)
data_pid_groupd['Доля однодневок %'] = data_pid_groupd['Доля однодневок %'].astype(str) + '%'

for_answer_data_subs[['Доля однодневок %', 
                      'Расходы на трафик $', 
                      'GGR', 
                      'Маржа без органики $', 
                      'Маржа от органики $', 
                      'Средние издержки %']] = for_answer_data_subs[['Доля однодневок %', 
                                                                     'Расходы на трафик $', 
                                                                     'GGR', 
                                                                     'Маржа без органики $', 
                                                                     'Маржа от органики $', 
                                                                     'Средние издержки %']].round(0)
for_answer_data_subs[['Доля однодневок %', 
                      'Расходы на трафик $', 
                      'GGR', 
                      'Маржа без органики $', 
                      'Маржа от органики $', 
                      'Средние издержки %']] = for_answer_data_subs[['Доля однодневок %', 
                                                                     'Расходы на трафик $', 
                                                                     'GGR', 
                                                                     'Маржа без органики $', 
                                                                     'Маржа от органики $', 
                                                                     'Средние издержки %']].astype(int)

for_answer_data_subs['Доля однодневок %'] = for_answer_data_subs['Доля однодневок %'].astype(str) + '%'
for_answer_data_subs['Средние издержки %'] = for_answer_data_subs['Средние издержки %'].astype(str) + '%'
data_pid_groupd['Средние издержки %'] = data_pid_groupd['Средние издержки %'].astype(str) + '%'

############################################## конец шлифовки данных ########################################


@dp.message(CommandStart())
async def cmd_start(message: types.Message):
    if message.from_user.id in ALLOWED_USERS:
        await message.reply(
            
f'Привет {message.from_user.first_name}!\nПрисылай мне сообщения в формате "/info Пид Гео Саб" через пробел и без ковычек, так же обрати внимание на то, что значение Гео должно быть написано в верхнем регистре. Пример :\n/info 181781 KZ @aiydkaa\n\nТак же обрати пожалуйста внимание на то, что если у пида пустое значение саба мне его нужно писать в сообщение символом нижнего подчекивания.\nПример:\n/info 8412 AT _\n\nЕсли тебе требуется информация только по конкретному пиду, в конкретном гео, без разбивки по сабам, то в качестве значения саба просто пиши плюсик\nПример :\n/info 8412 AT +'
                                
                                )
    else:
        await message.reply(
            
            f'Извините, у вас нет доступа к этому боту.\nТвой ID: {message.from_user.id}'
                                
                                )

# Взаимодействие с сообщением от пользователя
@dp.message(Command(commands="info"))
async def send_picture(message: types.Message):
    if message.from_user.id in ALLOWED_USERS:
        # Получаем полный текст сообщения, удаляя лишние пробелы
        text = message.text.strip()
        # Извлекаем команду и аргументы
        command, *args = text.split(maxsplit=1)
        # Если аргументы есть, обрабатываем их
        if args:
            args = args[0]  # Получаем строку аргументов
            await message.reply(f'Вы запросили данные для: {args}')

            try:
                partner_id, geo, sub_id = args.split()
                partner_id = int(partner_id)
                if me_info[me_info['partner_id'] == partner_id].shape[0] == 0:
                    await message.reply('Партнера нет в справочнике me_info')
                else:
                    if Acti_me_info[Acti_me_info['partner_id'] == partner_id].shape[0] != 0:
                        await message.reply('По таким пидам модель в процессе разработки.')

                    elif Fmedia_me_info[Fmedia_me_info['partner_id'] == partner_id].shape[0] != 0:
                        await message.reply('На текущий момент он не делится по гео.')

                    else:
                        if sub_id == '+':
                            #
                            sss = (
                                    data_pid_groupd.loc[(data_pid_groupd['Веб'] == partner_id) &
                                                        (data_pid_groupd['Гео'] == geo)].
                                    sort_values(by='Когорта', ascending=False).
                                    reset_index(drop=True)
                                  )

                            if sss.shape[0] == 0:
                                await message.reply(
'За последние шесть когорт нет данных в текущей модели или ошибка в написании, пожалуйста прочитай /start')

                            else:
                                # Создание копии DataFrame
                                df_copy = sss.copy()
                                df_copy.loc[(df_copy['Прогноз окупаемости мес.'] == 'никогда'), 
                                            'Прогноз окупаемости мес.'] = 999999
                                df_copy['Прогноз окупаемости мес.'] = (
                                                                        df_copy['Прогноз окупаемости мес.'].
                                                                        astype(int)
                                                                        )
                                payback_last_week = sss.loc[(sss['Когорта'] == sss['Когорта'].max()), 
                                                            'Прогноз окупаемости мес.'].values[0]
                                persent_one_day_player_last_week = sss.loc[
                                                                    (sss['Когорта'] == sss['Когорта'].max()), 
                                                                   'Доля однодневок %'].values[0]
                                red_w = df_copy[df_copy['Прогноз окупаемости мес.'] >= 19].shape[0]
                                yellow_w = df_copy.loc[(df_copy['Прогноз окупаемости мес.'] >= 11) 
                                                       & (df_copy['Прогноз окупаемости мес.'] < 19)].shape[0]
                                grean_w = df_copy[df_copy['Прогноз окупаемости мес.'] < 11].shape[0]
                                grean_w_not_nul_fee = df_copy.loc[(df_copy['Прогноз окупаемости мес.'] < 11) 
                                                                  & (df_copy['Расходы на трафик $'] != 0)
                                                                 ].shape[0]
                                count_yyyyww = len(list(sss['Когорта'].unique()))
                                global_count_yyyyww = len(
                                                        list(for_count_yyyyww_data.loc[
                                                            (for_count_yyyyww_data['partner_id']==partner_id) 
                                                            & (for_count_yyyyww_data['geo'] == geo), 
                                                      'yyyyww'].unique()))
                                # размер изображения
                                fig, ax = plt.subplots(figsize=(15, 3))  # размер фигуры
                                # цвет фона фигуры
                                fig.patch.set_facecolor('mediumpurple')  # Например, светло-серый цвет

                                # Убираем оси
                                ax.axis('tight')
                                ax.axis('off')

                                # Создаем таблицу
                                table = ax.table(
                                                    cellText=sss.values, 
                                                    colLabels=sss.columns, 
                                                    cellLoc='center', 
                                                    loc='center'
                                                )

                                # Настройка стиля таблицы
                                table.auto_set_font_size(False)
                                table.set_fontsize(19)
                                table.auto_set_column_width(col=list(range(len(sss.columns))))
                                
                                # Настройка цвета фона для всех ячеек таблицы
                                for key, cell in table.get_celld().items():
                                    row, col = key
                                    if row == 0:
                                        cell.set_facecolor('purple')  # Цвет фона заголовка
                                        cell.set_text_props(color='lavenderblush')  # Цвет текста заголовка
                                    else:
                                        value = sss.iloc[row - 1, col]
                                        if col == sss.columns.get_loc('Прогноз окупаемости мес.'):
                                            if value == 'никогда':
                                                cell.set_facecolor('red')  # Цвет фона для 'никогда'
                                                cell.set_text_props(color='white', 
                                                                    fontsize=19)  # Цвет текста
                                            elif isinstance(value, (int, float)):
                                                if value <= 10:
                                                    cell.set_facecolor('lime')  # Зеленый цвет
                                                    cell.set_text_props(color='dimgray', 
                                                                        fontsize=19)  # Цвет текста
                                                elif 11 <= value <= 18:
                                                    cell.set_facecolor('orange')  # Желтый цвет
                                                    cell.set_text_props(color='black', 
                                                                        fontsize=19) # Текст 
                                                else:
                                                    cell.set_facecolor('red')  # Красный цвет
                                                    cell.set_text_props(color='white', 
                                                                        fontsize=19) # Текст 

                                        else: # Фиолетовый цвет для всех остальных ячеек
                                            cell.set_facecolor('darkorchid')  
                                            cell.set_text_props(color='ivory', 
                                                                fontsize=19)  # Цвет текста 

                                # Настройка таблицы, чтобы занять всю фигуру
                                table.scale(1, 2)  # Масштабирование по вертикали и горизонтали
                                plt.title(
                                    
f'Данные окупаемости последних шести когорт. Модель в БД от : {date_str} \nПид : {partner_id}, Гео : {geo}',
                                    
                                            color='seashell', 
                                            fontsize=20, 
                                            fontweight='bold'
                                        )
                                
                                # Сохраняем таблицу как изображение
                                image_path = 'table_image.png'
                                plt.savefig(image_path, bbox_inches='tight', dpi=300)
                                plt.close()  # Закрываем фигуру, чтобы освободить память

                                # Отправки файла
                                await bot.send_document(
                                    chat_id=message.chat.id,
                                    document=types.FSInputFile(
                                        path=file_path,
                                        filename='PAYBACK_CHECK.png')
                                )
                                await message.reply(
                                    
f'Общее число когорт в модели : {global_count_yyyyww}\n\nДанные по последним шести когортам :\nЧисло когорт : {count_yyyyww}\n\nИз них : \nКоличество красных : {red_w}\nКоличество желтых : {yellow_w}\nОбщее количество зеленых когорт : {grean_w}\nКоличество зеленых с расходами : {grean_w_not_nul_fee}'
                                                        
                                                        )
                                
                                await message.reply(
                                    
f'Окупаемость в крайнюю неделю составляет : {payback_last_week} месяцев и доля однодневок равна {persent_one_day_player_last_week}'
                                                    )
                                
                                if grean_w > yellow_w + red_w:
                                    await message.reply(
                                        
                                        f'В целом окупаемость хорошая и находится в зеленой зоне'
                                    
                                    )
                                elif red_w > grean_w + yellow_w:
                                    await message.reply(
                                        
                                        f'По данным последних шести когорт окупаемость плохая'
                                        
                                                            )
                                else:
                                    await message.reply(
                                        
f'Окупаемость находится в желтой зоне или, по данным последних шести когорт, сильно изменчива. Для деталей смотрите PAYBACK_CHECK.png'
                                                            )

                        else:
                            sub_id = sub_id if sub_id != '_' else ''
                            sss = (
                                    for_answer_data_subs.loc[
                                              (for_answer_data_subs['Веб'] == partner_id)
                                            & (for_answer_data_subs['Гео'] == geo) 
                                            & (for_answer_data_subs['sub_id'] == sub_id)].
                                                        sort_values(by='Когорта', 
                                                                    ascending=False).
                                                        reset_index(drop=True)
                                        )

                            if sss.shape[0] == 0:
                                await message.reply(
                                    
f'За последние шесть когорт нет данных в текущей модели или ошибка в написании, пожалуйста прочитай /start'
                                
                                                        )
                            else:
                                # Создание копии DataFrame
                                df_copy = sss.copy()
                                df_copy.loc[(df_copy['Прогноз окупаемости мес.'] == 'никогда'), 
                                            'Прогноз окупаемости мес.'] = 999999
                                df_copy['Прогноз окупаемости мес.'] = (
                                                                        df_copy['Прогноз окупаемости мес.'].
                                                                            astype(int)
                                                                            )
                                payback_last_week = sss.loc[(sss['Когорта'] == sss['Когорта'].max()), 
                                                            'Прогноз окупаемости мес.'].values[0]
                                persent_one_day_player_last_week = (sss.loc[
                                                                    (sss['Когорта'] == sss['Когорта'].max()), 
                                                                     'Доля однодневок %'].values[0])
                                # Считаем дополнительную информацию
                                red_w = df_copy[df_copy['Прогноз окупаемости мес.'] >= 19].shape[0]
                                yellow_w = df_copy.loc[(df_copy['Прогноз окупаемости мес.'] >= 11) 
                                                       & (df_copy['Прогноз окупаемости мес.'] < 19)].shape[0]
                                grean_w = df_copy[df_copy['Прогноз окупаемости мес.'] < 11].shape[0]
                                grean_w_not_nul_fee = df_copy.loc[
                                                            (df_copy['Прогноз окупаемости мес.'] < 11) 
                                                          & (df_copy['Расходы на трафик $'] != 0)].shape[0]
                                count_yyyyww = len(list(sss['Когорта'].unique()))
                                global_count_yyyyww = len(
                                                        list(
                                                        for_count_yyyyww_data.loc[
                                                        (for_count_yyyyww_data['partner_id'] == partner_id) 
                                                      & (for_count_yyyyww_data['geo'] == geo) 
                                                      & (for_count_yyyyww_data['sub_id'] == sub_id),
                                                        'yyyyww'].unique()
                                                               )
                                                                )

                                # Размер изображения
                                fig, ax = plt.subplots(figsize=(15, 3))  # Размер фигуры
                                fig.patch.set_facecolor('mediumpurple')  # Цвет

                                # Убераем оси
                                ax.axis('tight')
                                ax.axis('off')

                                # Создаем таблицу
                                table = ax.table(cellText=sss.values, 
                                                 colLabels=sss.columns, 
                                                 cellLoc='center', 
                                                 loc='center')

                                # Настройка стиля таблицы
                                table.auto_set_font_size(False)
                                table.set_fontsize(19)
                                table.auto_set_column_width(col=list(range(len(sss.columns))))
                            
                                # Настройка цвета фона для всех ячеек таблицы
                                for key, cell in table.get_celld().items():
                                    row, col = key
                                    if row == 0:
                                        cell.set_facecolor('purple')  # Цвет фона заголовка
                                        cell.set_text_props(color='lavenderblush')  # Цвет текста заголовка
                                    else:
                                        value = sss.iloc[row - 1, col]
                                        if col == sss.columns.get_loc('Прогноз окупаемости мес.'):
                                            if value == 'никогда':
                                                cell.set_facecolor('red')  # Цвет фона для 'никогда'
                                                cell.set_text_props(color='white', 
                                                                    fontsize=19)  # Цвет текста
                                            elif isinstance(value, (int, float)):
                                                if value <= 10:
                                                    cell.set_facecolor('lime')  # Зеленый цвет
                                                    cell.set_text_props(color='dimgray', 
                                                                        fontsize=19) # Цвет текста
                                                elif 11 <= value <= 18:
                                                    cell.set_facecolor('orange')  # Желтый цвет
                                                    cell.set_text_props(color='black', 
                                                                        fontsize=19) # Цвет текста
                                                else:
                                                    cell.set_facecolor('red')  # Красный цвет
                                                    cell.set_text_props(color='white', 
                                                                        fontsize=19) # Цвет текста
                                        else:
                                            cell.set_facecolor('darkorchid')  # Цвет для всех остальных ячеек
                                            cell.set_text_props(color='ivory', 
                                                                fontsize=19) # Цвет текста остальных

                                # Настройка таблицы, чтобы занять всю фигуру
                                table.scale(1, 2)  # Масштабирование по вертикали и горизонтали
                                plt.title(
                                    
f'Данные окупаемости последних шести когорт. Модель в БД от : {date_str}\nПид : {partner_id}, Гео : {geo}, Саб : {sub_id}',
                                    
                                            color='seashell', 
                                            fontsize=20, 
                                            fontweight='bold'
                                                 )

                                # Сохраняем таблицу как изображение
                                image_path = 'table_image.png'
                                plt.savefig(image_path, bbox_inches='tight', dpi=300)
                                plt.close()  # Закрываем фигуру, чтобы освободить память

                                # Отправки файла
                                await bot.send_document(
                                    chat_id=message.chat.id,
                                    document=types.FSInputFile(
                                        path=file_path,
                                        filename='PAYBACK_CHECK.png'))
                                await message.reply(
                                    
f'Общее число когорт в модели : {global_count_yyyyww}\n\nДанные по последним шести когортам :\nЧисло когорт : {count_yyyyww}\n\nИз них : \nКоличество красных : {red_w}\nКоличество желтых : {yellow_w}\nОбщее количество зеленых когорт : {grean_w}\nКоличество зеленых с расходами : {grean_w_not_nul_fee}'
                                                        
                                                        )
                                await message.reply(
                                    
f'Окупаемость в крайнюю неделю составляет : {payback_last_week} месяцев и доля однодневок равна {persent_one_day_player_last_week}'
                                
                                                        )
                                if grean_w > yellow_w + red_w:
                                    await message.reply(
                                        
                                        f'В целом окупаемость хорошая и находится в зеленой зоне'
                                    
                                                            )
                                elif red_w > grean_w + yellow_w:
                                    await message.reply(
                                        
                                        f'По данным последних шести когорт окупаемость плохая'
                                                            
                                                            )
                                else:
                                    await message.reply(
                                        
f'Окупаемость находится в желтой зоне или, по данным последних шести когорт, сильно изменчива. Для деталей смотрите PAYBACK_CHECK.png'\
                                    
                                                            )

            except ValueError:
                await message.reply(
                    
f'Ошибка в формате ввода. Убедитесь, что вы отправляете три значения через пробел.\n/start'
                                        
                                        )
            except Exception as e:
                await message.reply(f'Произошла ошибка: {e}')

        else:
            await message.reply(f'Извините, вы не указали аргументы для запроса')
    else:
        await message.reply(f'Извините, у вас нет доступа к этому боту.\nТвой ID: {message.from_user.id}')




# Дополнительная настройка Бота
async def main():
    await dp.start_polling(bot)


if __name__ == '__main__':
     #logging.basicConfig(level=logging.INFO) # вывод логов, при необходимости раскомментировать
    try:
        asyncio.run(main())
    except KeyboardInterrupt:  # чтобы не было ошибки при остановке бота
        print('Exit')
