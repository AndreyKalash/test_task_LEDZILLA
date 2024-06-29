from datetime import date, datetime
from typing import Any, Dict, List

import requests
from dateutil.relativedelta import relativedelta
from sqlalchemy import Table
from sqlalchemy.orm import Session


def get_wb_data(TOKEN: str) -> List[Dict[str, Any]]:
    """
    Получает данные о заказах от поставщика с внешнего API Wildberries.

    Args:
        TOKEN (str): Токен авторизации для доступа к API.

    Returns:
        dict: JSON-данные, содержащие информацию о заказах от поставщика.
    """
    # определение даты для запроса (от текущей даты до 4 месяцев назад)
    curr_date = date.today()
    date_from = curr_date - relativedelta(months=4)

    # формирование заголовков и параметров запроса
    headers = {"Authorization": TOKEN}
    params = {"dateFrom": date_from}

    url = "https://statistics-api-sandbox.wildberries.ru/api/v1/supplier/orders"
    # выполнение GET-запроса к API с указанными заголовками и параметрами
    resp = requests.get(url, headers=headers, params=params)

    return resp.json()


def rows_from_data(data) -> List[Dict[str, Any]]:
    """
    Преобразует данные из формата API в строки (словари) для вставки в базу данных.

    Args:
        data (List[Dict[str, Any]]): Список словарей с данными о заказах от поставщика.

    Returns:
        List[Dict[str, Any]]: Список словарей, каждый словарь представляет собой строку данных
        для вставки в таблицу базы данных.
    """

    # формирование текущей даты и времени для поля "Upload Date"
    upload_date = str(datetime.now())
    # список для хранения строк
    rows = []
    for d in data:
        # формирование строки для вставки в базу данных
        row = {
            "nmid": d["nmId"],
            "warehousename": d["warehouseName"],
            "regionname": d["regionName"],
            "gnumber": d["gNumber"],
            "srid": d["srid"],
            "date": d["date"],
            "Upload Date": upload_date,
            "Name": "Калашников Андрей",
        }
        rows.append(row)

    return rows


def insert_data_to_table(
    session: Session, wb_api_table: Table, data: List[Dict[str, Any]]
) -> None:
    """
    Вставляет данные в таблицу WB API.

    Args:
        session (Session): Сессия SQLAlchemy для выполнения запросов к базе данных.
        wb_api_table (Table): Объект таблицы WB API.
        data (List[Dict[str, Any]]): Список словарей, представляющих данные для вставки в таблицу.

    Returns:
        None
    """
    try:
        insert_stmt = wb_api_table.insert().values(data)
        session.execute(insert_stmt)
        session.commit()
    except Exception as e:
        session.rollback()
        print(e)
    finally:
        session.close()


def task2_main(session: Session, TOKEN: str, wb_api_table: Table) -> None:
    """
    Основная функция для выполнения задачи 2.

    Получает данные с WB API, формирует строки данных для вставки в таблицу и загружает их в базу данных.

    Args:
        session (Session): Сессия SQLAlchemy для выполнения запросов к базе данных.
        TOKEN (str): Токен для доступа к внешнему API.
        wb_api_table (Table): Объект таблицы WB API, в которую будут вставлены данные.

    Returns:
        None
    """
    # получение данных с WB API
    data = get_wb_data(TOKEN)

    # формирование строк для вставки в таблицу
    rows = rows_from_data(data)

    # вставка данных в таблицу
    insert_data_to_table(session, wb_api_table, rows)
