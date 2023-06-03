# Moex Research
Простая библиотека для работы с данными [API MOEX](https://iss.moex.com/iss/reference/).

# Installation
Скачайте [данный пакет](https://github.com/IlyaBurkhanov/moex_research/tree/main/api_lib), и используйте его, 
в своих ноутбуках.

Если потребуется, установите зависимости (pandas & requests):
```
pip install -r requirements.txt
```
Библиотека работает с версией Python >= 3.7

# Описание библиотеки.
Общее число эндпроинтов доступных в API MOEX превышает 150 шт. 

1. Часть из них не работает, устарела, или доступна только при оформлении подписки;
2. Множество эндпоинты не имеют описания или внятного описания;
3. Для запроса к эндпоинтам нужно знать множество глобальных сущностей, таких как рынки, типы инструментов, площадки, 
режимы торгов и прочее;
4. У каждого эндпоинта множество параметров запроса, со своими ограничениями. Неконтролируемые ошибки в запросе 
параметров могут прислать некорректный результат;
5. Каждый эндпоинт может возвращать множество сущностей, излишнюю информацию, например, метаданные и полный перечень 
атрибутов, часто не требуемых для анализа;
6. Часть эндпоинтов имеют курсоры, а часть возвращает данные без пагинации. Требуется дублировать запросы для получения
полного объема информации.

## **Все перечисленные проблемы решает данная библиотека**

С деталями вы можете ознакомиться [в приложенных ноутбуках](https://github.com/IlyaBurkhanov/moex_research/tree/main/notebooks):

Подготовки справочника API:
- [Изучение API MOEX](https://github.com/IlyaBurkhanov/moex_research/blob/main/notebooks/API_MOEX_PART_1.ipynb)
- [Подготовка справочника](https://github.com/IlyaBurkhanov/moex_research/blob/main/notebooks/API_MOEX_PART_2.ipynb)

Ознакомление и примеры использования:
- [Знакомство с библиотекой](https://github.com/IlyaBurkhanov/moex_research/blob/main/notebooks/API_MOEX_PART_3.ipynb)
- [Примеры решаемых аналитических задач](https://github.com/IlyaBurkhanov/moex_research/blob/main/notebooks/API_MOEX_PART_4.ipynb)


# Примеры использования
.. code-block:: python

    from api_lib import MoexApi
    MOEX = MoexApi()
    MOEX.help()
.. code-block::
    Для запроса данных используйте метод request!
    Список базовых сущностей хранится в переменной 'available_entities'
    Виды бумаг хранятся в переменной 'SECTYPE'
    Срочность опциона хранится в переменной 'OPTION_SERIES_TYPE'
    Все доступные API можно посмотреть вызвав all_api()
    Описание API можно посмотреть вызвав api_description
    Описание глобальной сущности можно получить вызвав description