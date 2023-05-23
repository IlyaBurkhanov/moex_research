import datetime
from abc import abstractmethod
from typing import Any, Union


class MoexParamCheckerMixin:
    CHECK_METHODS = {
        "q": "check_instrument_find",
        "engine": "check_engine",
        "is_trading": "check_bool",
        "market": "check_market",
        "group_by": "check_group_by",
        "limit": "check_limit",
        "start": "check_start",
        "date": "check_date",
        "is_traded": "check_bool",
        "hide_inactive": "check_bool",
        "securitygroups": "check_security_group",
        "trade_engine": "check_engine",
        "time": "check_time",
        "asset_type": "check_asset_type",
        "sort_order": "check_sort_order",
        "tradingsession": "check_session",
        "security_collection": "check_security_collection",
        "type": "check_type",
        "latest": "check_bool",
        "only_actual": "check_bool",
        "securities": "check_securities",
        "boardid": "check_boards",
        "from": "check_date",
        "till": "check_date",
        "status": "check_status",
        "numtrades": "check_int",
        "interim": "check_bool",
        "assetcode": "check_instrument_find",
        "sort_column": "check_sort_columns",
        "primary_board": "check_bool",
        "assets": "check_assets",
        "index": "check_bool",
        "previous_session": "check_bool",
        "first": "check_int",
        "leaders": "check_bool",
        "nearest": "check_bool",
        "sectypes": "check_sectype",
        "tradeno": "check_int",
        "reversed": "check_bool",
        "recno": "check_bool",
        "next_trade": "check_bool",
        "yielddatetype": "check_yielddatetype",
        "interval": "check_interval",
        "iss.reverse": "check_bool_like_bool",
        "year": "check_int",
        "month": "check_int",
        "expiration_date": "check_date",
        "option_type": "check_option_type",
        "series_type": "check_option_series_type",
        "tickers": "check_instrument_find",
    }

    current_endpoint_id: int = -1

    # Добавить в основной класс
    set_engine: set = set()
    set_security_group: set = set()
    set_session: set[int] = set()
    set_security_collection: set = set()
    set_board: set = set()
    set_security_type: set = set()
    set_duration: set[int] = set()

    set_market: set = {"EQ", "FI", "MX"}
    set_group_by: set = {"group", "type"}
    set_asset_type: set = {"S", "F"}
    set_sort_order: set = {"asc", "desc"}
    set_type: set = {"daily", "monthly"}
    set_status = {"traded", "nottraded", "all"}
    set_yielddatetype = {"MBS", "MATDATE", "OFFERDATE"}
    set_option_type = {"C", "P"}

    @abstractmethod
    def get_endpoint_columns(self) -> set:
        """
        У многих эндпоинтов есть сортировка по столбцу. Столбцы мы уже получили в json формате ранее.
        Подготовим их чуть позже и сохраним в отдельном модуле.
        """

    @staticmethod
    def check_instrument_find(q: Any):
        if not isinstance(q, str):
            q = str(q)
        for word in q.split():
            if len(word) < 3:
                raise ValueError("Запрос инструментов длиной менее трёх букв игнорируются.")
        return q

    def check_engine(self, engine: str):
        if engine not in self.set_engine:
            raise ValueError(f"Engine: '{engine}' не найден")
        return engine

    @staticmethod
    def check_bool(val: Any):
        if val:
            return 1
        return 0

    def check_market(self, market: str):
        upper_market = market.upper()
        if upper_market not in self.set_market:
            raise ValueError("Доступны: EQ - индекс акций, FI - индекс облигаций, MX - составные индексы")
        return upper_market

    def check_group_by(self, group_by: str):
        lower_group_by = group_by.lower()
        if lower_group_by not in self.set_group_by:
            raise ValueError("Доступны значения group и type")
        return lower_group_by

    @staticmethod
    def check_limit(self, limit: int):
        if not isinstance(limit, int):
            raise ValueError("Лимит должен быть целым числом")
        return limit

    @staticmethod
    def check_start(self, start: int):
        if not isinstance(start, int):
            raise ValueError("Курсор должен быть целым числом")
        return start

    @staticmethod
    def check_date(value_date: Union[str, datetime.date, datetime.datetime]):
        if isinstance(value_date, (datetime.date, datetime.datetime)):
            return value_date.strftime(value_date, '%Y-%m-%d')
        try:
            datetime.datetime.strptime(value_date, '%Y-%m-%d')
            return value_date
        except:
            raise ValueError(f"Значение даты {value_date} не является датой или не удовлетворяет формату ГГГГ-ММ-ДД")

    def check_security_group(self, security_group: str):
        if security_group not in self.set_security_group:
            raise ValueError(f"Группа {security_group} не найдена.")
        return security_group

    @staticmethod
    def check_time(value_time: Union[str, datetime.time, datetime.datetime]):
        if isinstance(value_time, (datetime.time, datetime.datetime)):
            return value_time.strftime(value_time, '%H:%M:%S')
        try:
            datetime.datetime.strptime(value_time, '%H:%M:%S')
            return value_time
        except:
            raise ValueError(f"Значение времени {value_time} не удовлетворяет формату ЧЧ:ММ:CC")

    def check_asset_type(self, asset_type: str):
        upper_asset_type = asset_type.upper()
        if upper_asset_type not in self.set_asset_type:
            raise ValueError("Доступны фильтры по типу базового актива. (S - Опционы на акцию, F - Опционы на фьючерс)")
        return upper_asset_type

    def check_sort_order(self, sort_order: str):
        lower_sort_order = sort_order.lower()
        if lower_sort_order not in self.set_sort_order:
            raise ValueError('Направление сортировки. "asc" - По возрастанию значения, "desc" - По убыванию!')
        return lower_sort_order

    def check_session(self, session: Union[str, int]):
        if isinstance(session, str):
            if session.isdigit():
                session = int(session)
        if session in self.set_session:
            return session
        raise ValueError('Укажите корректную сессию: 0 - Утренняя;  1 - Основная;  2 - Вечерняя;  3 - Итого')

    def check_security_collection(self, security_collection: str):
        if security_collection not in self.set_security_collection:
            raise ValueError(f"Группа ФИ '{security_collection}' не найдена.")
        return security_collection

    def check_type(self, value_type: str):
        lower_value_type = value_type.lower()
        if lower_value_type not in self.set_type:
            raise ValueError("Не верный тип капитализации. Доступные значения: daily, monthly")
        return lower_value_type

    @staticmethod
    def check_securities(securities: Union[str, List[str]], max_security=10):
        if isinstance(securities, list) and len(securities) > max_security:
            raise ValueError(f"Запросить можно не более {max_security} фин. инструментов")
        return securities

    def check_boards(self, boards: Union[str, List[str]]):
        if isinstance(boards, str):
            boards = [boards]
        for board in boards:
            if board not in self.set_board:
                raise ValueError(f"Площадка '{board}' не найдена.")
        return boards

    def check_status(self, status: str):
        lower_status = status.lower()
        if lower_status not in self.set_status:
            raise ValueError(f"Ошибка cтатуса. Укажите фильтр торгуемости инструментов: traded, nottraded или all")
        return lower_status

    @staticmethod
    def check_int(value: Union[str, int]):
        if isinstance(value, int) or (isinstance(value, str) and value.isdigit()):
            return value
        raise ValueError(f"Передан признак [{value}], который должен быть целым числом!!! ")

    def check_sort_columns(self, column):
        available_columns: set = self.get_endpoint_columns()
        if column not in available_columns:
            raise ValueError(f"Не найден атрибут сортировки {column} в доступных {sorted(available_columns)}")
        return column

    def check_assets(self, securities: Union[str, List[str]]):
        return self.check_securities(securities, 5)

    def check_sectype(self, values: Union[str, List[str]]):
        warnings.warn(
            """
            Поле 'sectypes' не соответствует значениям из глобального справочника. Для ПФИ указывается краткий код БА,
            например, si, ri, mx и т.д. Для спота обратитесть к справочнику 'SECTYPE' объекта.
            """
        )
        if isinstance(values, list) and len(values) > 5:
            raise ValueError(f"Запросить можно не более 5 типов фин. инструментов")
        elif isinstance(values, str):
            values = [values]
        for value in values:
            if len(value) > 1:
                continue
            if value not in self.SECTYPE:
                raise ValueError(f"Код {value} не найден в справочнике типов фин. инструментов")
        return values

    def check_yielddatetype(self, yielddatetype: str):
        upper_yielddatetype = yielddatetype.upper()
        if upper_yielddatetype not in self.set_yielddatetype:
            raise ValueError(f"Фильтр доступен по типам доходности: MBS, MATDATE, OFFERDATE")
        return upper_yielddatetype

    def check_interval(self, interval: int):
        if interval not in self.set_duration:
            raise ValueError("Интервал должен соответствовать доступным значениям. Смотрите справочник 'durations'.")
        return interval

    def check_bool_like_bool(self, value: Any):
        if self.check_bool(value):
            return "true"
        return "false"

    def check_option_type(self, option_type: str):
        upper_option_type = option_type.upper()
        if upper_option_type not in self.set_option_type:
            raise ValueError("Не верный тип опциона. C - CALL, P - PUT")
        return upper_option_type

    def check_option_series_type(self, series_type: str):
        upper_series_type = series_type.upper()
        if upper_series_type not in self.OPTION_SERIES_TYPE:
            raise ValueError("Не найдена серия опциона. Обратитесь к справочнику OPTION_SERIES_TYPE")
        return upper_series_type