import datetime
import warnings

from abc import abstractmethod
from contextvars import ContextVar
from typing import Any, Union, List

from api_lib.dictionaries import OPTION_SERIES_TYPE, SECTYPE

used_api_id: ContextVar[str] = ContextVar("used_api_id")  # for futures objects


# FIXME: DRY IT AFTER FUNCTIONAL TEST !!!
class MoexParamCheckerMixin:
    _used_api_id = used_api_id
    _check_params = {
        "q": "_check_instrument_find",
        "engine": "_check_engine",
        "is_trading": "_check_bool",
        "market": "_check_market",
        "group_by": "_check_group_by",
        "limit": "_check_limit",
        "start": "_check_start",
        "date": "_check_date",
        "is_traded": "_check_bool",
        "hide_inactive": "_check_bool",
        "securitygroups": "_check_security_group",
        "trade_engine": "_check_engine",
        "time": "_check_time",
        "asset_type": "_check_asset_type",
        "sort_order": "_check_sort_order",
        "tradingsession": "_check_session",
        "security_collection": "_check_security_collection",
        "type": "_check_type",
        "latest": "_check_bool",
        "only_actual": "_check_bool",
        "securities": "_check_securities",
        "boardid": "_check_boards",
        "from": "_check_date",
        "till": "_check_date",
        "status": "_check_status",
        "numtrades": "_check_int",
        "interim": "_check_bool",
        "assetcode": "_check_instrument_find",
        "sort_column": "_check_sort_columns",
        "primary_board": "_check_bool",
        "assets": "_check_assets",
        "index": "_check_bool",
        "previous_session": "_check_bool",
        "first": "_check_int",
        "leaders": "_check_bool",
        "nearest": "_check_bool",
        "sectypes": "_check_sectype",
        "tradeno": "_check_int",
        "reversed": "_check_bool",
        "recno": "_check_bool",
        "next_trade": "_check_bool",
        "yielddatetype": "_check_yielddatetype",
        "interval": "_check_interval",
        "iss.reverse": "_check_bool_like_bool",
        "year": "_check_int",
        "month": "_check_int",
        "expiration_date": "_check_date",
        "option_type": "_check_option_type",
        "series_type": "_check_option_series_type",
        "tickers": "_check_instrument_find",
    }

    _check_entity = {
        "asset": "_check_asset_ba",
        "board": "_check_board_entity",
        "boardgroup": "_check_board_group",
        "collection": "_check_security_collection",
        "datatype": "_check_datatype",
        "engine": "_check_engine",
        "event_id": "_check_int",
        "indexid": "_check_indexid",
        "market": "_check_markets",
        "news_id": "_check_int",
        "security": "_check_instrument_find",
        "securitygroup": "_check_security_group",
        "session": "_check_session",
        "year": "_check_int",
    }

    # Добавить в основной класс
    _set_boards: set = set()
    _set_boardgroups: set = set()
    _set_datatypes: set = set()
    _set_durations: set[int] = set()
    _set_engines: set = set()
    _set_indexids: set = set()
    _set_markets: set = set()
    _set_report_names: set = set()
    _set_securitycollections: set = set()
    _set_securitygroups: set = set()
    _set_securitytypes: set = set()
    _set_sessions: set[int] = set()

    # other_params
    _set_market: set = {"EQ", "FI", "MX"}
    _set_group_by: set = {"group", "type"}
    _set_asset_type: set = {"S", "F"}
    _set_sort_order: set = {"asc", "desc"}
    _set_type: set = {"daily", "monthly"}
    _set_status = {"traded", "nottraded", "all"}
    _set_yielddatetype = {"MBS", "MATDATE", "OFFERDATE"}
    _set_option_type = {"C", "P"}

    @abstractmethod
    def _get_endpoint_columns(self) -> set:
        """
        У многих эндпоинтов есть сортировка по столбцу. Столбцы мы уже получили в json формате ранее.
        Подготовим их чуть позже и сохраним в отдельном модуле.
        """
        raise NotImplementedError("Нужно запилить метод получения доступных колонок")

    def _checker(self, key: str, value: Any, is_params=True):
        if not is_params and not isinstance(value, (int, str)):
            raise ValueError(f"Параметр пути сущности {key} принимает значение отличное от строки: {value}")
        use_func = self._check_params.get(key) if is_params else self._check_entity.get(key)
        if use_func is None:
            raise KeyError(f"Параметр: {key} не найден в словаре валидации! Проверьте библиотеку.")
        check_func = getattr(self, use_func, None)
        if check_func is None:
            raise KeyError(f"Не найден метод валидации для переданного параметра: {key}. Проверьте библиотеку.")
        return check_func(value)

    @staticmethod
    def _check_instrument_find(q: Any) -> str:
        if not isinstance(q, str):
            q = str(q)
        for word in q.split():
            if len(word) < 3:
                raise ValueError("Запрос инструментов длиной менее трёх букв игнорируются.")
        return q

    def _check_engine(self, engine: str) -> str:
        if engine not in self._set_engines:
            raise ValueError(f"Engine: '{engine}' не найден")
        return engine

    @staticmethod
    def _check_bool(val: Any) -> int:
        if val:
            return 1
        return 0

    def _check_market(self, market: str) -> str:
        upper_market = market.upper()
        if upper_market not in self._set_market:
            raise ValueError("Доступны: EQ - индекс акций, FI - индекс облигаций, MX - составные индексы")
        return upper_market

    def _check_group_by(self, group_by: str) -> str:
        lower_group_by = group_by.lower()
        if lower_group_by not in self._set_group_by:
            raise ValueError("Доступны значения group и type")
        return lower_group_by

    @staticmethod
    def _check_limit(limit: int) -> int:
        if not isinstance(limit, int):
            raise ValueError("Лимит должен быть целым числом")
        return limit

    @staticmethod
    def _check_start(start: int) -> int:
        if not isinstance(start, int):
            raise ValueError("Курсор должен быть целым числом")
        return start

    @staticmethod
    def _check_date(value_date: Union[str, datetime.date, datetime.datetime]) -> str:
        if isinstance(value_date, (datetime.date, datetime.datetime)):
            return value_date.strftime('%Y-%m-%d')
        try:
            datetime.datetime.strptime(value_date, '%Y-%m-%d')
            return value_date
        except ValueError:
            raise ValueError(f"Значение даты {value_date} не является датой или не удовлетворяет формату ГГГГ-ММ-ДД")

    def _check_security_group(self, security_group: str) -> str:
        if security_group not in self._set_securitygroups:
            raise ValueError(f"Группа {security_group} не найдена.")
        return security_group

    @staticmethod
    def _check_time(value_time: Union[str, datetime.time, datetime.datetime]) -> str:
        if isinstance(value_time, (datetime.time, datetime.datetime)):
            return value_time.strftime('%H:%M:%S')
        try:
            datetime.datetime.strptime(value_time, '%H:%M:%S')
            return value_time
        except ValueError:
            raise ValueError(f"Значение времени {value_time} не удовлетворяет формату ЧЧ:ММ:CC")

    def _check_asset_type(self, asset_type: str) -> str:
        upper_asset_type = asset_type.upper()
        if upper_asset_type not in self._set_asset_type:
            raise ValueError("Доступны фильтры по типу базового актива. (S - Опционы на акцию, F - Опционы на фьючерс)")
        return upper_asset_type

    def _check_sort_order(self, sort_order: str) -> str:
        lower_sort_order = sort_order.lower()
        if lower_sort_order not in self._set_sort_order:
            raise ValueError('Направление сортировки. "asc" - По возрастанию значения, "desc" - По убыванию!')
        return lower_sort_order

    def _check_session(self, session: Union[str, int]) -> int:
        if isinstance(session, str):
            if session.isdigit():
                session = int(session)
        if session in self._set_sessions:
            return session
        raise ValueError('Укажите корректную сессию: 0 - Утренняя;  1 - Основная;  2 - Вечерняя;  3 - Итого')

    def _check_security_collection(self, security_collection: str) -> str:
        if security_collection not in self._set_securitycollections:
            raise ValueError(f"Группа ФИ '{security_collection}' не найдена.")
        return security_collection

    def _check_type(self, value_type: str) -> str:
        lower_value_type = value_type.lower()
        if lower_value_type not in self._set_type:
            raise ValueError("Не верный тип капитализации. Доступные значения: daily, monthly")
        return lower_value_type

    @staticmethod
    def _check_securities(securities: Union[str, List[str]], max_security=10) -> Union[str, List[str]]:
        if isinstance(securities, list) and len(securities) > max_security:
            raise ValueError(f"Запросить можно не более {max_security} фин. инструментов")
        return securities

    def _check_boards(self, boards: Union[str, List[str]]) -> List[str]:
        if isinstance(boards, str):
            boards = [boards]
        for board in boards:
            if board not in self._set_boards:
                raise ValueError(f"Площадка '{board}' не найдена.")
        return boards

    def _check_board_entity(self, board: str) -> str:
        if board not in self._set_boards:
            raise ValueError(f"Площадка '{board}' не найдена.")
        return board

    def _check_status(self, status: str) -> str:
        lower_status = status.lower()
        if lower_status not in self._set_status:
            raise ValueError(f"Ошибка cтатуса. Укажите фильтр торгуемости инструментов: traded, nottraded или all")
        return lower_status

    @staticmethod
    def _check_int(value: Union[str, int]) -> Union[str, int]:
        if isinstance(value, int) or (isinstance(value, str) and value.isdigit()):
            return value
        raise ValueError(f"Передан признак [{value}], который должен быть целым числом!!! ")

    def _check_sort_columns(self, column: str) -> str:
        available_columns: set = self._get_endpoint_columns()
        if column not in available_columns:
            raise ValueError(f"Не найден атрибут сортировки {column} в доступных {sorted(available_columns)}")
        return column

    def _check_assets(self, securities: Union[str, List[str]]) -> Union[str, List[str]]:
        return self._check_securities(securities, 5)

    @staticmethod
    def _check_sectype(values: Union[str, List[str]]) -> List[str]:
        warnings.warn(
            """
            Поле 'sectypes' не соответствует значениям из глобального справочника. Для ПФИ указывается краткий код БА,
            например, si, ri, mx и т.д. Для спота обратитесь к справочнику 'SECTYPE'.
            """
        )
        if isinstance(values, list) and len(values) > 5:
            raise ValueError(f"Запросить можно не более 5 типов фин. инструментов")
        elif isinstance(values, str):
            values = [values]
        for value in values:
            if len(value) > 1:
                continue
            if value not in SECTYPE:
                raise ValueError(f"Код {value} не найден в справочнике типов фин. инструментов")
        return values

    def _check_yielddatetype(self, yielddatetype: str) -> str:
        upper_yielddatetype = yielddatetype.upper()
        if upper_yielddatetype not in self._set_yielddatetype:
            raise ValueError(f"Фильтр доступен по типам доходности: MBS, MATDATE, OFFERDATE")
        return upper_yielddatetype

    def _check_interval(self, interval: int) -> int:
        if interval not in self._set_durations:
            raise ValueError("Интервал должен соответствовать доступным значениям. Смотрите справочник 'durations'.")
        return interval

    def _check_bool_like_bool(self, value: Any) -> str:
        if self._check_bool(value):
            return "true"
        return "false"

    def _check_option_type(self, option_type: str) -> str:
        upper_option_type = option_type.upper()
        if upper_option_type not in self._set_option_type:
            raise ValueError("Не верный тип опциона. C - CALL, P - PUT")
        return upper_option_type

    @staticmethod
    def _check_option_series_type(series_type: str) -> str:
        upper_series_type = series_type.upper()
        if upper_series_type not in OPTION_SERIES_TYPE:
            raise ValueError("Не найдена серия опциона. Обратитесь к справочнику OPTION_SERIES_TYPE")
        return upper_series_type

    @staticmethod
    def _check_asset_ba(asset: str) -> str:
        if len(asset) < 2:
            raise ValueError("Длина кода базового актива не может быть меньше 2")
        return asset

    def _check_board_group(self, board_group: str) -> str:
        if board_group not in self._set_boardgroups:
            raise ValueError(f"Группа площадок {board_group} не найдена в справочнике")
        return board_group

    def _check_datatype(self, datatype: str) -> str:
        if datatype not in self._set_datatypes:
            raise ValueError(f"Datatype может принимать значения securities или trades. Передано: {datatype}")
        return datatype

    def _check_indexid(self, index_id: str) -> str:
        if index_id not in self._set_indexids:
            raise ValueError(f"'{index_id}' не найден в справочнике индексов")
        return index_id

    def _check_markets(self, market: str) -> str:
        if market not in self._set_markets:
            raise ValueError(f"'{market}' не найден в справочнике доступных рынков")
        return market
