from typing import Any, Tuple

import pandas as pd
from sqlalchemy import Row, Sequence, Table, case, func
from sqlalchemy.orm import Session


def create_query(
    session: Session, sales: Table, supply: Table
) -> Tuple[str, Sequence[Row[Any]]]:
    """
    Формирует и выполняет запрос для решения первой задачи.

    Args:
        session (Session): Сессия SQLAlchemy для выполнения запросов к базе данных.
        sales (Table): Объект таблицы SALES.
        supply (Table): Объект таблицы SUPPLY.

    Returns:
        tuple: Кортеж из SQL-кода запроса и результата выполнения запроса (результат fetchall).
    """

    supply_cte = session.query(
        supply.c["Product ID"],
        supply.c["Supply QTY"],
        supply.c["Costs Per PCS"],
        supply.c["#Supply"],
        func.row_number()
        .over(partition_by=supply.c["Product ID"], order_by=supply.c["#Supply"])
        .label("Supply_Rank"),
    ).cte("Supply_CTE")

    sales_cte = session.query(
        sales.c["Product ID"],
        sales.c["Sales QTY"],
        sales.c["Date"],
        func.row_number()
        .over(partition_by=sales.c["Product ID"], order_by=sales.c["Date"])
        .label("Sales_Rank"),
    ).cte("Sales_CTE")

    matched_cte = (
        session.query(
            supply_cte.c["Product ID"],
            supply_cte.c["Supply QTY"],
            supply_cte.c["Costs Per PCS"],
            supply_cte.c["#Supply"],
            sales_cte.c["Sales QTY"],
            sales_cte.c["Date"],
            sales_cte.c["Sales_Rank"],
            supply_cte.c["Supply_Rank"],
            func.row_number()
            .over(
                partition_by=supply_cte.c["Product ID"],
                order_by=[supply_cte.c["Product ID"], sales_cte.c["Date"]],
            )
            .label("Match_Rank"),
        )
        .join(sales_cte, supply_cte.c["Product ID"] == sales_cte.c["Product ID"])
        .filter(sales_cte.c["Sales_Rank"] >= supply_cte.c["Supply_Rank"])
        .cte("Matched_CTE")
    )

    fifo_costs_cte = (
        session.query(
            matched_cte.c["Product ID"],
            matched_cte.c["Date"],
            func.min(matched_cte.c["#Supply"]),
            matched_cte.c["Sales QTY"],
            matched_cte.c["Supply QTY"],
            matched_cte.c["Costs Per PCS"],
            (
                matched_cte.c["Costs Per PCS"]
                * case(
                    (
                        matched_cte.c["Sales QTY"] <= matched_cte.c["Supply QTY"],
                        matched_cte.c["Sales QTY"],
                    ),
                    else_=matched_cte.c["Supply QTY"],
                )
            ).label("Costs"),
            case(
                (
                    matched_cte.c["Sales QTY"] <= matched_cte.c["Supply QTY"],
                    matched_cte.c["Sales QTY"],
                ),
                else_=matched_cte.c["Supply QTY"],
            ).label("Qty_Sold"),
        )
        .group_by(
            matched_cte.c["Product ID"],
            matched_cte.c["Date"],
            matched_cte.c["Sales QTY"],
            matched_cte.c["Supply QTY"],
            matched_cte.c["Costs Per PCS"],
        )
        .order_by(matched_cte.c["Date"], func.min(matched_cte.c["#Supply"]))
        .cte("FIFO_Costs_CTE")
    )

    final_query = (
        session.query(
            fifo_costs_cte.c["Product ID"],
            func.to_char(fifo_costs_cte.c["Date"], "yyyy-mm").label("YearMonth"),
            func.sum(fifo_costs_cte.c["Costs"]).label("Total_Costs"),
        )
        .group_by(
            fifo_costs_cte.c["Product ID"],
            func.to_char(fifo_costs_cte.c["Date"], "yyyy-mm"),
        )
        .order_by(
            fifo_costs_cte.c["Product ID"],
            func.to_char(fifo_costs_cte.c["Date"], "yyyy-mm"),
        )
    )

    sql_code = str(final_query)
    report = session.execute(final_query).fetchall()

    return sql_code, report


def task1_main(
    session: Session,
    sales: Table,
    supply: Table,
    sql_save_path: str,
    excel_save_path: str,
) -> None:
    """
    Основная функция для выполнения задачи 1.

    Формирует SQL-запрос и отчет на основе данных из таблиц.
    Сохраняет отчет в формате Excel и SQL-запрос.

    Args:
        session (Session): Сессия SQLAlchemy для выполнения запросов к базе данных.
        sales (Table): Объект таблицы SALES.
        supply (Table): Объект таблицы SUPPLY.
        sql_save_path (str): Путь для сохранения SQL-запроса.
        excel_save_path (str): Путь для сохранения отчета в Excel.

    Returns:
        None
    """
    # создание SQL-запроса и получения данных
    sql_code, report = create_query(session, sales, supply)

    # сохранение отчета в Excel
    pd.DataFrame(report).to_excel(excel_save_path)

    # сохранение SQL-запроса
    with open(sql_save_path, "w") as f:
        f.write(sql_code)
