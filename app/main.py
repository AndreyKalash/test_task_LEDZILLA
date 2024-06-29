from configparser import ConfigParser, SectionProxy
from typing import List, Tuple

from sqlalchemy import MetaData, Table, create_engine
from sqlalchemy.orm import Session, sessionmaker

from task1 import task1_main
from task2 import task2_main


def get_configs(config_path: str, section_names: List[str]) -> List[SectionProxy]:
    """
    Получает секции из файла конфигурации и возвращает список объектов SectionProxy.

    Args:
        config_path (str): Путь к файлу конфигурации.
        section_names (List[str]): Список имен секций, которые необходимо получить.

    Returns:
        List[SectionProxy]: Список объектов SectionProxy, каждый из которых представляет собой секцию конфигурации.

    Raises:
        FileNotFoundError: Если указанный файл конфигурации не найден.
    """
    config = ConfigParser()
    config.read(config_path)

    sections = []
    for sn in section_names:
        s = config[sn]
        sections.append(s)

    return sections


def init_tables(d: SectionProxy, table_names: List[str]) -> Tuple[Session, List[Table]]:
    """
    Инициализирует соединение с базой данных и возвращает сессию SQLAlchemy и список таблиц.

    Args:
        d (SectionProxy): Секция конфигурации с параметрами подключения к базе данных.
        table_names (List[str]): Список имен таблиц, которые необходимо инициализировать.

    Returns:
        tuple: Кортеж из SQLAlchemy сессии и списка объектов Table для указанных таблиц.

    Raises:
        Exception: Если возникает ошибка при подключении к базе данных или загрузке таблиц.
    """
    # cоздание объекта метаданных для указанной схемы
    metadata = MetaData(d["SCHEMA"])

    # создание подключения к базе данных PostgreSQL
    engine = create_engine(
        f"postgresql+psycopg2://{d['DB_USER']}:{d['DB_PASS']}@{d['DB_HOST']}:{d['DB_PORT']}/{d['DB_NAME']}?sslmode={d['DB_SSLM']}"
    )

    # создание сессии SQLAlchemy для выполнения операций
    Session = sessionmaker(bind=engine)
    session = Session()

    tables = []
    # загрузка указанных таблицы в список
    for tn in table_names:
        table = Table(tn, metadata, autoload_with=engine)
        tables.append(table)

    return session, tables


def main() -> None:
    """
    Основная функция для выполнения задач.

    Читает конфигурацию, инициализирует подключение к базе данных и загружает необходимые таблицы.
    Запускает две задачи (task1_main и task2_main).
    """
    # получение секций конфигурации для базы данных и токена WB API
    sections = get_configs("config.ini", ["database", "wb_api"])
    d, w = sections

    # инициализация соединения с бд и загрузка таблиц
    session, tables = init_tables(d, ["SALES", "SUPPLY", "WB API"])

    # пути для сохранения SQL-скрипта и Excel-файла
    sql_save_path = "script.sql"
    excel_save_path = "report.xlsx"
    # токен для работы с внешним API
    TOKEN = w["TOKEN"]

    task1_main(session, tables[0], tables[1], sql_save_path, excel_save_path)
    task2_main(session, TOKEN, tables[2])


if __name__ == "__main__":
    main()
