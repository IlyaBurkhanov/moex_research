import pandas as pd
import requests
import json
import time
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
        print("Для запроса данных используйте метод request!")
        print("Список базовых сущностей хранится в переменной 'available_entities'")
        print("Виды бумаг хранятся в переменной 'SECTYPE'")
        print("Срочность опциона хранится в переменной 'OPTION_SERIES_TYPE'")
        print("Все доступные API можно посмотреть вызвав all_api()")
        print("Описание API можно посмотреть вызвав api_description")
        print("Описание глобальной сущности можно получить вызвав description")

    def __new__(cls, *args, **kwargs):
        if cls.__instance is None:
            cls.__instance = super().__new__(cls, *args, **kwargs)
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

    def description(self, entity: str, name: str) -> str:
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
            raise KeyError(f"Не найден api c id={api_id}")
        return result

    def _get_full_endpoint(self, api_dict: dict, request_params: dict) -> str:
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

    def _get_param_for_endpoint(self, api_dict: dict, request_params: dict) -> dict:
        use_params = {}
        use_params.update(dictionaries.ENDPOINT_DEFAULTS.get(self._used_api_id.get(), {}))
        api_params = set(api_dict.get("params", []))
        if api_params:
            for param in list(request_params):
                if param in api_params:
                    param_value = request_params.pop(param)
                else:
                    drop_alpha = (
                        1 if param.startswith("_") else
                        2 if param.startswith("P_") else
                        3 if param.find("__") != -1 else
                        0
                    )
                    if drop_alpha:
                        param_value = request_params.pop(param)
                        if drop_alpha != 3:
                            param = param[drop_alpha:]
                        else:
                            param = param.replace("__", ".")
                        if param not in api_params:
                            raise KeyError(f"Параметр {param} не используется данным эндпоинтом!")
                    else:
                        continue
                if isinstance(param_value, (list, set)) and param not in dictionaries.PARAMS_ALLOWED_MANY:
                    raise ValueError(f"Параметр {param} не может принимать множественные значения!")
                use_params[param] = self._checker(param, param_value)
        return use_params

    @staticmethod
    def _get_use_columns(data_entities: set, request_params: dict) -> dict:
        use_columns = {}
        for param in list(request_params):
            if param.startswith("COLUMNS_"):
                columns = request_params.pop(param)
                if not isinstance(columns, list):
                    raise ValueError(f"Колонки должны быть перечислены в списке! Передано: {columns}")
                param = param[8:]
                if param not in data_entities:
                    raise KeyError(f"Блок {param} не возвращается в данном эндпоинте!")
                use_columns[f"{param}.columns"] = columns
        return use_columns

    @staticmethod
    def _get_block_params(data_entities: set, blocks: list) -> dict:
        use_block = []
        for block in blocks:
            if block not in data_entities:
                raise ValueError(f"Блок {block} не возвращается данным эндпоинтом!")
            use_block.append(block)
        return {"iss.only": use_block}

    @staticmethod
    def _request(url: str, use_params: dict) -> dict:
        request = requests.get(url, params=use_params)
        if request.status_code != 200:
            raise requests.RequestException("Запрос вернул статус <> 200 (OK)")
        # print(request.url)
        return request.json()

    @staticmethod
    def _dataframe_create(final_data: dict) -> Union[dict, pd.DataFrame]:
        result = {key: pd.DataFrame(data=value["data"], columns=value["columns"]) for key, value in final_data.items()}
        return result[list(result)[0]] if len(result) == 1 else result

    def _request_with_cursor(self, url: str, api_dict: dict, use_params: dict) -> Union[dict, pd.DataFrame]:
        tmp_result = {}
        cursor = api_dict["cursor_name"]
        cursor_entity = cursor.replace(".cursor", "")
        use_params.setdefault("start", 0)

        request = self._request(url, use_params)
        for entity, value in request.items():
            if entity != cursor:
                tmp_result[entity] = {"data": value["data"], "columns": value["columns"]}

        if cursor_entity not in request:
            return self._dataframe_create(tmp_result)

        cursor_idx = request[cursor]["columns"].index
        index_id, total_id, page_size_id = cursor_idx("INDEX"), cursor_idx("TOTAL"), cursor_idx("PAGESIZE")
        cursor_data = request[cursor]["data"][0]
        use_params["iss.only"] = ','.join([cursor_entity, cursor])
        page_size = cursor_idx("PAGESIZE")
        cnt = 1
        while cnt < dictionaries.MAX_REQ_PER_QUERY and cursor_data[total_id] > cursor_data[index_id] + page_size:
            time.sleep(dictionaries.SLEEP_TIME)
            use_params["start"] += page_size
            request = self._request(url, use_params)
            tmp_result[cursor_entity]["data"].extend(request[cursor_entity]["data"])
            cursor_data = request[cursor]["data"][0]
            cnt += 1
        if cnt >= dictionaries.MAX_REQ_PER_QUERY:
            warnings.warn(f"К API стучались {cnt} раз. Возвращены не все данные!!!")
        return self._dataframe_create(tmp_result)

    def _request_without_cursor(self, url: str, use_params: dict) -> Union[dict, pd.DataFrame]:
        # FIXME: Множество поинтов без курсора нужно изучать детально, так как возвращаемые сущности могут быть
        # без пагинации. Запрос уходит в бесконечный цикл и возвращаются дубликаты.
        # Костылем является проверка последней записи предыдущего запроса с последней записью текущего
        tmp_result = {}
        use_params.setdefault("start", 0)
        requested_entity = set()
        max_cnt = 0
        for entity, value in self._request(url, use_params).items():
            tmp_result[entity] = {"data": value["data"], "columns": value["columns"]}
            if max_cnt < len(value["data"]):
                max_cnt = len(value["data"])
            if entity.find(".") == -1 and len(value["data"]) != 0:
                requested_entity.add(entity)

        use_params["iss.only"] = ','.join(requested_entity)
        cnt = 1
        while cnt < dictionaries.MAX_REQ_PER_QUERY:
            if len(requested_entity) == 0:
                break
            use_params["start"] += max_cnt
            is_change_iss_only = False
            time.sleep(dictionaries.SLEEP_TIME)

            for entity, value in self._request(url, use_params).items():
                if len(value["data"]) == 0 or value["data"][-1] == tmp_result[entity]["data"][-1]:  # FIXME: костыль!
                    requested_entity.remove(entity)
                    is_change_iss_only = True
                else:
                    tmp_result[entity]["data"].extend(value["data"])
            cnt += 1
            if is_change_iss_only:
                use_params["iss.only"] = ','.join(requested_entity)
        else:
            warnings.warn(f"К API стучались {cnt} раз. Возвращены не все данные!!!")

        return self._dataframe_create(tmp_result)

    def _request_to_api(self, api_dict: dict, url: str, use_params: dict) -> Union[dict, pd.DataFrame]:
        for param in list(use_params):  # For multivalue use comma separator
            if isinstance(use_params[param], (list, set)):
                use_params[param] = ','.join([str(request_value) for request_value in use_params[param]])
        has_cursor = api_dict.get("has_cursor")
        cursor_name = api_dict.get("cursor_name")
        has_start_param = api_dict.get("params", {}).get("start")
        if not has_cursor or not has_start_param:
            return self._dataframe_create(self._request(url, use_params))

        if cursor_name:
            return self._request_with_cursor(url, api_dict, use_params)
        return self._request_without_cursor(url, use_params)

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
            2.3. Если имя параметра является ключевым атрибутом python, добавьте к имени '_', например: _from.
            2.4. Если в имени параметра содержится '.' (точка), замените ее на двойное подчеркивание, например:
            iss.reverse -> iss__reverse
        3. Если хотим указать атрибутный состав полей для блока, то к имени блока добавляем префикс
        'COLUMNS_'. Названия атрибутов перечисляем в списке. Например, COLUMNS_boardgroups = ["slug", "is_default"].
        :return: Если запрашивается только одна сущность то вернется фрейм данных. Если много, то в словаре, где
        ключом является имя сущности.
        """
        api_dict = self._check_and_get_api_dict(api_id)
        self._used_api_id.set(str(api_id))
        url = self._get_full_endpoint(api_dict, kwargs)
        use_params = self._get_param_for_endpoint(api_dict, kwargs)

        if blocks or kwargs:
            data_entities = set(api_dict.get("return_data", []))
            if kwargs:
                use_params.update(self._get_use_columns(data_entities, kwargs))
            if blocks:
                use_params.update(self._get_block_params(data_entities, blocks))

        if kwargs:
            raise KeyError(f"Переданы неопределенные для API параметры: {kwargs}")

        if only_market_data:
            use_params["iss.data"] = "on"
        use_params.update(dictionaries.DEFAULT_GET_PARAMS)
        return self._request_to_api(api_dict, url, use_params)
