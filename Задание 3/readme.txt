Часть 1: Оптимизированная загрузка в БД
1. Напишите ETL-скрипт:
- Читает CSV-файл с заказами (1 млн строк).
- Выполняет нормализацию данных (выносит курсы и предметы в справочные таблицы).
- Использует batch-загрузку (COPY вместо INSERT).
- Работает асинхронно (например, asyncpg для PostgreSQL).
Файлы с решением:
    - task_3_1.py
    - ddl.sql

2. Как ускорить загрузку данных?
- Какие параметры БД (work_mem, shared_buffers) можно настроить?
    Настройки WAL:
        - Можно записывать в unlogged таблицы(без записи в WAL), если потеря данных при сбое допускается
        - увеличить max_wal_size
        - увеличить checkpoint_timeout
    Настройки памяти:
        - увеличить shared_buffers, чтобы избежать работы с диском
        - увеличить work_mem: если логика загрузки сложная и требуется производить sql запросы к целевым таблицам
        - увеличить maintenance_work_mem, чтобы ускорить операции обслуживания после вставки

- Как избежать блокировок таблиц при массовой вставке?
    - если целостность загрузки не важна, то можно вставлять батчи в отдельных транзакциях
    - вставлять во временную таблицу
    - при использовании COPY FROM запросы на чтение не блокируются



Часть 2: Оптимизация SQL-запросов
1. Напишите SQL-запрос, который возвращает ТОП-5 самых продаваемых курсов по месяцам.
    файл task_3_2.sql

2. Напишите SQL-запрос, который возвращает ТОП-3 самых популярных пакетов по предметам.
    файл task_3_2.sql

3. Оптимизируйте запросы:
- Какие индексы добавить?
    - Оба запроса выбирают большое кол-во строк относительно общего кол-ва строк в таблицах,
    а значит индексы в классических СУБД(например btree в postgres) будут неэффективны,
    и планировщик предпочтет им последовательное сканирование.

- Какую стратегию партиционирования выбрать (range, hash)?
    Можно использовать range по дате (например в разбивке по месяцам), но это будет работать только 
    если в условии запроса присутствует ключ партиционирования и запросы затрагивают небольшое кол-во партиций.

- Как минимизировать full table scan?
    В основном планировщик сам определяет оптимальный план выполнения и минимизирует кол-во строк в шагах запроса.
    В случае, если запрос слишком большой и планировщик не может выбрать оптимальный план, 
    он будет выполнять запрос последовательно. В таком случае следует обеспечить следует писать запрос так, 
    чтобы на каждом шаге запроса было отброшено как можно большее кол-во строк.


Часть 3: Работа с неструктурированными данными

В базе данных имеется неструктурированный список курсов, представленный в разных форматах и названиях. Ваша задача — разработать алгоритм нормализации названий курсов, чтобы они соответствовали единому стандарту.
Описание входных данных:
У вас есть фрагмент таблицы, содержащей сырые данные о курсах и предметах. Ваш алгоритм должен приводить их к категориям представленным в Приложении 1.
Требования к решению:
1. Разработать Python-скрипт, который:

Принимает в качестве входных данных pandas.DataFrame с сырыми названиями курсов и предметов.
Применяет нормализацию названий курсов с помощью словаря соответствий и шаблонов.
Генерирует новые столбецы standardized_course и standardized_subject  с категориями курсов и предметов.

2. Учесть обработку ошибок:
Учитывать случаи, когда название курса или предмета отсутствует.
Обрабатывать лишние пробелы и регистр (lower() и strip()).
Учитывать возможность появления латинских букв.
Обрабатывать неявные наименования курсов (например, "2 месяца полугодового платинум и марафон в подарок" → Спецкурс).

3. Оптимизировать процесс классификации:
Использовать регулярные выражения (regex) для поиска паттернов.
Учитывать возможные изменения названий в будущем.



4. Вывести финальный DataFrame с результатами обработки.
 	Дополнительные требования:
1. Код должен быть хорошо структурирован, с логичными функциями и комментариями.
2. Предоставьте unit-тесты (например, через pytest) для проверки работы алгоритма.
3. Оцените производительность кода, если количество строк > 100 000.

Файл с решением: task_3_3
