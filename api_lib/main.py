import pandas as pd
import requests
import json
import warnings
from collections import namedtuple
from os import path
from typing import Union, List, Dict, Any

import api_lib.dictionaries as dictionaries
from api_lib.mixins import MoexParamCheckerMixin

DICT_JSON = "MOEX_API_DICT.json"
PATH_TO_DICT = path.join(path.dirname(__file__), DICT_JSON)

with open(PATH_TO_DICT, "r") as jsn_file:
    GLOBAL_API = json.load(jsn_file)


class MoexApi(MoexParamCheckerMixin):
    __instance = None
    __base_attr = namedtuple("__base_attr", "main description")

    __ENDPOINT_DEFAULTS = dictionaries.ENDPOINT_DEFAULTS
    __PARAMS_ALLOWED_MANY = dictionaries.PARAMS_ALLOWED_MANY
    SECTYPE = dictionaries.SECTYPE
    OPTION_SERIES_TYPE = dictionaries.OPTION_SERIES_TYPE
    datatypes = dictionaries.DATATYPES
    report_names = dictionaries.REPORT_NAMES
    sessions = dictionaries.SESSIONS

    __main_entities = {
        "engines": __base_attr("name", "title"),
        "markets": __base_attr("market_name", "market_title"),
        "boards": __base_attr("boardid", "board_title"),
        "boardgroups": __base_attr("name", "title"),
        "durations": __base_attr("interval", "title"),
        "securitytypes": __base_attr("security_type_name", "security_type_title"),
        "securitygroups": __base_attr("name", "title"),
        "securitycollections": __base_attr("name", "title"),
    }

    available_entities = sorted(list(__main_entities) + ["datatypes", "report_names", "sessions", "indexids"])

    @staticmethod
    def help() -> None:
        print("Список базовых сущностей хранится в переменной 'available_entities'")
        print("Описание сущности возвращается методом 'description'")
        print("Виды бумаг хранятся в переменной 'SECTYPE'")
        print("Срочность опциона хранится в переменной 'OPTION_SERIES_TYPE'")

    def __new__(cls, *args, **kwargs):
        if cls.__instance is None:
            cls._instance = super().__new__(cls, *args, **kwargs)
        return cls.__instance

    def __init__(self):
        moex_dict = requests.get(dictionaries.DICTIONARY_API_URL).json()
        for entity, columns in self.__main_entities.items():
            setattr(
                self,
                entity,
                pd.DataFrame(
                    data=moex_dict[entity]["data"], columns=moex_dict[entity]["columns"]
                ).set_index(columns.main),
            )
        self.indexids = self._get_index_id()
        self._set_set_dictionaries()

    @staticmethod
    def _get_index_id() -> pd.DataFrame:
        index_ids = requests.get(dictionaries.INDEX_ID_API_URL).json()
        return pd.DataFrame(
            data=index_ids["indices"]["data"], columns=index_ids["indices"]["columns"]
        ).set_index("indexid")

    def description(self, entity: str, name) -> str:
        """
        Получить описание сущности.
        :param entity: тип сущности в единственном числе.
        :param name: имя сущности.
        :return: Описание сущности.
        """
        entities = entity + "s"
        use_entity = getattr(self, entities, None)
        if use_entity is None:
            raise NameError(f"Entity '{entity}' is not found!")
        if not isinstance(use_entity, pd.DataFrame):
            raise ValueError(f"Entity '{entity}' hasn't description")
        if entities in self.__main_entities:
            return getattr(self, entities).loc[name, (self.__main_entities[entities].description,)]
        if entities in {"report_names", "sessions"}:
            return getattr(self, entities).loc[name, ("description",)]
        if entities == "indexids":
            return getattr(self, entities).loc[name, ("shortname",)]
        raise NameError(f"In library added new entity {entity}. Please update this method!!!")

    @staticmethod
    def all_api():
        """
        :return: Возврат id и описания каждого эндпоинта.
        """
        for idx, values in sorted(GLOBAL_API.items(), key=lambda data: int(data[0])):
            print(idx, end=" ")
            print(values["description"])

    @staticmethod
    def api_description(api_id: Union[str, int], full_description: bool = False):
        """
        Описание эндпоинтов. Используйте данный метод как справочник для формирования запросов.
        :param api_id: id эндпоинта.
        :param full_description: True/False. Дополнительно вернет ссылку на описание эндпоинта и его template.
        :return: Описание возвращаемых сущностей, требуемых сущностей и параметров запроса.
        """
        api = GLOBAL_API.get(str(api_id))
        if api is None:
            raise KeyError(f"API with {api_id} not found!")

        print(f"{api_id}: {api['description']}")

        global_entities = api.get("global_entities")
        if global_entities:
            print(f"Глобальные сущности запроса: {global_entities}")

        print("\nВозвращаемые поинтом данные:")
        for name, value in api["return_data"].items():
            print(f"{name}: {value.get('description', '')}")

        params = api.get("params")
        if params:
            print("\nПараметры запроса:")
            for param, values in params.items():
                print(f"``{param}``:", end=" ")
                if values.get("description"):
                    print(values["description"], end=". ")
                if values.get("default"):
                    print(f"Значение по умолчанию: {values['default']}")
                else:
                    print()

        if api.get("has_cursor"):
            print("\nЭндпоинт возвращает ограниченное количество записей")

        if full_description:
            print(f"\nДетали: {api['faq_url']}")
            print(f"Темплейт: {api['endpoint']}")

    def _set_set_dictionaries(self):
        for entity in self.available_entities:
            data = getattr(self, entity)
            if isinstance(data, pd.DataFrame):
                setattr(self, f"_set_{entity}", set(data.index))
            else:
                setattr(self, f"_set_{entity}", set(data))

    def _get_endpoint_columns(self) -> set:
        use_api_id = self._used_api_id.get()
        columns = set()
        for column_data in GLOBAL_API[use_api_id]["return_data"].values():
            columns.update(column_data.get("columns", []))
        return columns

    @staticmethod
    def _check_and_get_api_dict(api_id: Union[int, str]) -> dict:
        result = GLOBAL_API.get(str(api_id))
        if result is None:
            raise ValueError(f"Не найден api c id={api_id}")
        return result

    def _get_full_endpoint(self, api_dict: dict, request_params: dict):
        endpoint = api_dict["endpoint"] + ".json"
        global_entities = api_dict.get("global_entities")
        if not global_entities:
            return endpoint

        path_params = {}
        for param in global_entities:
            entity = request_params.pop(param, None)
            if entity is None:
                raise KeyError(f"Параметр {param} является частью пути. Укажите его в запросе!")
            path_params[param] = self._checker(param, entity, is_params=False)
        return endpoint.format(**path_params)

    def request(
            self,
            api_id: Union[int, str],
            only_market_data: bool = False,
            blocks: List[str] = None,
            **kwargs: Any,
    ) -> Union[pd.DataFrame, Dict[str, pd.DataFrame]]:
        """
        Запрос данных по API Мосбиржи:
        :param api_id: указывается id поинта запроса данных;
        :param only_market_data: включать или нет непосредственно рыночные данные. По умолчанию не указываем;
        :param blocks: ответ может содержать несколько блоков данных и этот параметр позволяет выбрать только нужные.
        См. список возвращаемых данных в справочнике эндпоинтов.
        :param kwargs:
        1. Обязательно указываем значения всех требуемых глобальных сущностей;
        2. Указываем значения параметров;
            2.1. Если имя параметра совпадает с именем глобальной сущности, то к имени параметра добавляем префикс 'P_',
            например, "P_market" для параметра "market";
            2.2. Если значений параметров несколько, передаем их в виде списка, например, securities = ["GAZP", "LKOH"]
        3. Если хотим указать атрибутный состав полей для блока, то к имени блока добавляем префикс
        'COLUMNS_'. Названия атрибутов перечисляем в списке. Например, COLUMNS_boardgroups = ["slug", "is_default"].
        :return: Если запрашивается только одна сущность то вернется фрейм данных. Если много, то в словаре, где
        ключем является имя сущности.
        """
        api_dict = self._check_and_get_api_dict(api_id)
        self._used_api_id.set(str(api_id))
        url = self._get_full_endpoint(api_dict, kwargs)

