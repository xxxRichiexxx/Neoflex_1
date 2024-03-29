# Задание №1
В некотором банке внедрили новую frontend-систему для работы с клиентами, а так же обновили и саму базу данных. Большую часть данных успешно были перенесены из старых БД в одну новую централизованную БД.  Но в момент переключения со старой системы на новую возникли непредвиденные проблемы в ETL-процессе, небольшой период (конец 2017 начало 2018 года) так и остался в старой базе. Старую базу отключили, а не выгруженные данные сохранили в csv-файлы. Недавно банку потребовалось построить отчёт по 101 форме. Те данные что остались в csv-файлах тоже нужны. Загрузить их в новую БД не получиться из-за архитектурных и управленческих сложностей, нужно рассчитать витрину отдельно. Но для этого сначала нужно загрузить исходные данные из csv-файлов в детальный слой (DS) хранилища в СУБД Oracle / PostgreSQL.

## Задача 1.1
Разработать ETL-процесс для загрузки «банковских» данных из csv-файлов в соответствующие таблицы СУБД Oracle или PostgreSQL. Покрыть данный процесс логированием этапов работы и всевозможной дополнительной статистикой (на усмотрение вашей фантазии). В исходных файлах могут быть ошибки в виде некорректных форматах значений. Но глядя на эти значения вам будет понятно, какие значения имеются в виду.

### Исходные данные:
Данные из 6 таблиц в виде excel-файлов:
md_ledger_account_s – справочник балансовых счётов;
md_account_d – информация о счетах клиентов;
ft_balance_f – остатки средств на счетах;
ft_posting_f – проводки (движения средств) по счетам;
md_currency_d – справочник валют;
md_exchange_rate_d – курсы валют.
Файл «Структура таблиц.docx» – поможет создать таблицы в детальном слое DS.

### Требования к реализации задачи:
В своей БД создать пользователя / схему «DS».
Примеры команд:
https://oracle-dba.ru/docs/architecture/schemas/basics/
https://postgrespro.ru/docs/postgresql/9.6/sql-createschema
Создать в DS-схеме таблицы под загрузку данных из csv-файлов.
Начало и окончание работы процесса загрузки данных должно логироваться в специальную логовую таблицу. Эту таблицу нужно придумать самостоятельно;
После логирования о начале загрузки добавить таймер (паузу) на 5 секунд, чтобы чётко видеть разницу во времени между началом и окончанием загрузки. Из-за небольшого учебного объёма данных – процесс загрузки быстрый;
Для хранения логов нужно в БД создать отдельного пользователя / схему «LOGS» и создать в этой схеме таблицу для логов;
(В случае реализации процесса в Talend) В зависимости от мощностей рабочей станции – сделать загрузку из всех файлов одним потоком в параллели или отдельными потоками (может не хватить оперативной памяти для Java-heap);
Для корректного обновления данных в таблицах детального слоя DS нужно выбрать правильную Update strategy и использовать следующие первичные ключи для таблиц фактов, измерений и справочников (должно быть однозначное уникальное значение, идентифицирующее каждую запись таблицы):

Таблица	Первичный ключ
DS.FT_BALANCE_F	        ON_DATE, ACCOUNT_RK
DS.FT_POSTING_F	        OPER_DATE, CREDIT_ACCOUNT_RK, DEBET_ACCOUNT_RK
DS.MD_ACCOUNT_D	        DATA_ACTUAL_DATE, ACCOUNT_RK
DS.MD_CURRENCY_D	    CURRENCY_RK, DATA_ACTUAL_DATE
DS.MD_EXCHANGE_RATE_D	DATA_ACTUAL_DATE, CURRENCY_RK
DS.MD_LEDGER_ACCOUNT_S	LEDGER_ACCOUNT, START_DATE

### Технологические требования
ETL-процесс по загрузке файлов вы можете сделать с помощью различных технологий, которые вам будут удобней. Возможные варианты технологий:
Talend – бесплатная (для учебных целей) ETL-платформа;
Python – для данного языка существует множество библиотек, в том числе и для работы с базами данных и с различными файлами;
Java / Scala – для этих языков так же существует различные способы для работы с БД и файлами;
(*) Оркестрация процесса загрузки с помощью Airflow. Данный критерий не обязательный, но если вдруг вы сможете самостоятельно понять, установить и применить этот инструмент – это будет большим плюсом.

### Требования к демонстрации работы:
Записать видео с экрана компьютера, в котором вы демонстрируйте и комментируете в слух, то что вы делаете / уже разработали;
Нужно продемонстрировать создание или подробно прокомментировать разработанный вами поток (ETL-процесс);
Продемонстрировать, что поток работает – показать, что в таблице «DS.ft_balance_f» не было записей, потом запустить поток и показать, что таблица наполнилась;
Запись в таблицы должна выполняться в режиме «Запись или замена». Поэтому не забудьте определиьт ключевые поля для возможности обновлять информацию по уже существующим записям;
Продемонстрируйте как вы в файле «ft_balance_f.csv» меняете баланс для какого-нибудь <account_rk>, показываете что в таблице «DS.ft_balance_f» сперва была одна сумма у этого <account_rk> - потом запускаете ETL-процесс и показываете, что в таблице сумма обновилась;
Это видео загрузите к себе на облако (гугл-диск, яндекс-диск и т.д.) и предоставьте доступ по ссылке;
Приложите в домашнее задание внутри архива текстовый файл с ссылкой на ваше видео и все скрипты решения. В случае работы в Talend выгрузите и прикрепите поток.