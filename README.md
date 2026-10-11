# МЕТОДИЧЕСКОЕ ПОСОБИЕ
## ЧАСТЬ II: ОРГАНИЗАЦИЯ STAGING-СЛОЯ, АВТОМАТИЗАЦИЯ ВЕКТОРНОГО ELT И ПОСТРОЕНИЕ ВИТРИН ДАННЫХ В СРЕДЕ GOOGLE COLAB

---

### КОНТЕКСТ И АРХИТЕКТУРНЫЙ АНАЛИЗ

На предшествующем этапе в локальном инстансе PostgreSQL виртуальной машины Colab (`postgresql://dwh_admin:admin123@localhost:5432/elt_dwh`) была развернута базовая схема «Звезда» с элементами EAV (Entity-Attribute-Value):
* **Измерения:** `dim_company`, `dim_metric_dictionary`.
* **Факты:** `fact_financial_metrics`.

#### Выявленные архитектурные ограничения предыдущей реализации:
1. **Построчная итерация (Антипаттерн ETL):** Использование `df.iterrows()` на стороне Python для генерации отдельных SQL-запросов на каждую строку создает высокую сетевую/межпроцессную нагрузку и приводит к деградации производительности на больших объемах данных.
2. **Отсутствие Staging-буфера:** Данные валидировались и приводились к типам в памяти Python перед прямой записью в таблицы фактов. Нарушен принцип ELT, требующий сохранения исходного слепка сырых данных внутри СУБД.

#### Цель доработки:
Перевести пайплайн на классическую архитектуру **ELT**:
1. Выгрузка сырых данных пакетом (Batch Ingestion) в буферную таблицу слоя `staging`.
2. Внутрибазовое разрешение суррогатных ключей (`company_sk`, `metric_id`) и пакетный `UPSERT` единой SQL-транзакцией.
3. Развертывание реляционной витрины данных поверх EAV-структуры фактов с расчетом производных финансовых коэффициентов (EBITDA Margin, YoY Growth).

---

### [CODE ЯЧЕЙКА 1] ПРОВЕРКА ИНФРАСТРУКТУРЫ И АКТИВАЦИЯ СУБД

Выполните ячейку для проверки статуса демона PostgreSQL в ОС Ubuntu виртуальной машины Colab и инициализации пула подключений SQLAlchemy.

```python
import subprocess
import os

import pandas as pd
from sqlalchemy import create_engine, text

# 1. Проверка и перезапуск демона PostgreSQL при перезапуске runtime
status = subprocess.run(["service", "postgresql", "status"], capture_output=True, text=True)
if "online" not in status.stdout and "running" not in status.stdout:
    print("[INFO] Демон PostgreSQL остановлен. Выполняется запуск...")
    subprocess.run(["service", "postgresql", "start"], check=True)
    print("[OK] Демон PostgreSQL успешно запущен.")
else:
    print("[OK] Демон PostgreSQL активен.")

# 2. Инициализация пула соединений к локальной БД
# Явно указываем psycopg2 драйвер, чтобы избежать ModuleNotFoundError
DB_URI = 'postgresql+psycopg2://dwh_admin:admin123@localhost:5432/elt_dwh'
engine = create_engine(DB_URI, pool_pre_ping=True)

# 3. Тест коннекта
with engine.connect() as conn:
    version = conn.execute(text("SELECT version();")).scalar()
    current_db = conn.execute(text("SELECT current_database();")).scalar()
    print(f"[OK] Подключение установлено: База='{current_db}', Версия='{version.split()[0]} {version.split()[1]}'")
```

---

### [MARKDOWN ЯЧЕЙКА 2]
```markdown
## ЭТАП 8: ПРОЕКТИРОВАНИЕ И СОЗДАНИЕ STAGING-СЛОЯ (DDL)

В архитектуре ELT слой Staging принимает «сырые» данные без применения внешних внешних ключей (Foreign Keys) и жестких проверок ограничений целостности. 
Все поля принимаются в строковом или полуструктурированном виде, что исключает сбои загрузчика из-за несовпадения типов данных.
```

---

### [CODE ЯЧЕЙКА 2] DDL: Развертывание схемы staging
```python
staging_ddl = """
-- Создание изолированной схемы для буферных данных
CREATE SCHEMA IF NOT EXISTS staging;

-- Буферная таблица для входящих потоков финансовых отчетов
DROP TABLE IF EXISTS staging.raw_financial_feed;
CREATE TABLE staging.raw_financial_feed (
    raw_id BIGSERIAL PRIMARY KEY,
    report_date VARCHAR(50),
    ticker VARCHAR(20),
    company_name VARCHAR(255),
    gics_sector VARCHAR(100),
    metric_code VARCHAR(100),
    metric_type VARCHAR(50),
    metric_value VARCHAR(50),
    batch_loaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Индекс для оптимизации последующих этапов джойна при трансформации
CREATE INDEX idx_staging_feed_lookup ON staging.raw_financial_feed(ticker, metric_code);
"""

with engine.begin() as conn:
    conn.execute(text(staging_ddl))
    print("[OK] Схема 'staging' и буферная таблица 'raw_financial_feed' развернуты.")
```

---

### [MARKDOWN ЯЧЕЙКА 3]
```markdown
## ЭТАП 9: ПАКЕТНЫЙ EXTRACT & LOAD В STAGING (ВЫСОКОСКОРОСТНАЯ ЗАГРУЗКА)

Вместо медленного ручного импорта через UI Colab и построчного вызова SQL-запросов, загрузка реализуется программно:
1. Формируется пакет сырых данных (синтетический поток за расширенный период 2023-2024 гг.).
2. Данные загружаются напрямую в `staging.raw_financial_feed` методом высокоскоростной пакетной вставки (`COPY` / `multi-row INSERT`).
```

---

### [CODE ЯЧЕЙКА 3] Пакетная инжекция сырого потока данных
```python
import io

# 1. Формирование расширенного пакета данных (Mock Data Stream)
# Включает эмитентов: SBER, GAZP, LKOH, OZON, YNDX, ROSN
csv_stream_data = """report_date,ticker,company_name,gics_sector,metric_code,metric_type,metric_value
2023-12-31,LKOH,Lukoil,Energy,REVENUE,FINANCIAL,7928000.00
2023-12-31,LKOH,Lukoil,Energy,EBITDA,FINANCIAL,1800000.00
2023-12-31,LKOH,Lukoil,Energy,NET_PROFIT,FINANCIAL,1100000.00
2024-03-31,LKOH,Lukoil,Energy,REVENUE,FINANCIAL,2100000.00
2024-03-31,LKOH,Lukoil,Energy,EBITDA,FINANCIAL,450000.00
2024-03-31,LKOH,Lukoil,Energy,NET_PROFIT,FINANCIAL,280000.00
2023-12-31,ROSN,Rosneft,Energy,REVENUE,FINANCIAL,9163000.00
2023-12-31,ROSN,Rosneft,Energy,EBITDA,FINANCIAL,3005000.00
2023-12-31,ROSN,Rosneft,Energy,NET_PROFIT,FINANCIAL,1267000.00
2024-03-31,ROSN,Rosneft,Energy,REVENUE,FINANCIAL,2594000.00
2024-03-31,ROSN,Rosneft,Energy,EBITDA,FINANCIAL,857000.00
2024-03-31,ROSN,Rosneft,Energy,NET_PROFIT,FINANCIAL,399000.00
2023-12-31,OZON,Ozon Holdings,Consumer Discretionary,REVENUE,FINANCIAL,424000.00
2023-12-31,OZON,Ozon Holdings,Consumer Discretionary,EBITDA,FINANCIAL,-15000.00
2024-03-31,OZON,Ozon Holdings,Consumer Discretionary,REVENUE,FINANCIAL,122900.00
2024-03-31,OZON,Ozon Holdings,Consumer Discretionary,EBITDA,FINANCIAL,2500.00
2024-03-31,SBER,Sberbank,Financials,REVENUE,FINANCIAL,850000.00
2024-03-31,SBER,Sberbank,Financials,EBITDA,FINANCIAL,380000.00
2024-03-31,SBER,Sberbank,Financials,NET_PROFIT,FINANCIAL,350000.00
2024-03-31,GAZP,Gazprom,Energy,REVENUE,FINANCIAL,2150000.00
2024-03-31,GAZP,Gazprom,Energy,EBITDA,FINANCIAL,600000.00
2024-03-31,GAZP,Gazprom,Energy,NET_PROFIT,FINANCIAL,450000.00
2024-03-31,YNDX,Yandex,Information Technology,REVENUE,FINANCIAL,228000.00
2024-03-31,YNDX,Yandex,Information Technology,EBITDA,FINANCIAL,37000.00
2024-03-31,YNDX,Yandex,Information Technology,FREE_CASH_FLOW,FINANCIAL,45000.00
"""

# Чтение потока в буферный DataFrame
df_raw_payload = pd.read_csv(io.StringIO(csv_stream_data))

# 2. Пакетная загрузка в Staging (Load) через стандартный pandas to_sql со стратегией 'append'
# Параметр method='multi' упаковывает данные в крупные мультистрочные пакеты (INSERT VALUES (...), (...))
df_raw_payload.to_sql(
    name='raw_financial_feed',
    con=engine,
    schema='staging',
    if_exists='append',
    index=False,
    method='multi',
    chunksize=1000
)

# Проверка физической записи в Staging
with engine.connect() as conn:
    raw_count = conn.execute(text("SELECT COUNT(*) FROM staging.raw_financial_feed;")).scalar()
    print(f"[OK] Фаза LOAD завершена. В таблице staging.raw_financial_feed зафиксировано строк: {raw_count}")
```

---

### [MARKDOWN ЯЧЕЙКА 4]
```markdown
## ЭТАП 10: ВНУТРИБАЗОВАЯ ТРАНСФОРМАЦИЯ (ELT TRANSFORM)

Трансформация переносится на вычислительный движок СУБД. Вся бизнес-логика выполняется в рамках одной неделимой транзакции (`BEGIN ... COMMIT`):
1. **Дедупликация и очистка:** приведение строк к типам `DATE` и `NUMERIC`, удаление пробелов, регистронезависимая нормализация.
2. **Синхронизация измерений (`dim_company`):** добавление новых тикеров через `ON CONFLICT (ticker) DO UPDATE`.
3. **Синхронизация словаря (`dim_metric_dictionary`):** регистрация новых кодов метрик (`REVENUE`, `FREE_CASH_FLOW`) через `ON CONFLICT (metric_code) DO NOTHING`.
4. **Разрешение суррогатных ключей и загрузка фактов (`fact_financial_metrics`):** прямой реляционный `JOIN` между staging-слоем и обновленными справочниками измерений с защитой от дублирования связки `(company_sk, metric_id, report_date)`.
```

---

### [CODE ЯЧЕЙКА 4] SQL-пайплайн трансформации и наполнения витрин
```python
elt_transform_sql = r"""
BEGIN;

-- 1. СИНХРОНИЗАЦИЯ ИЗМЕРЕНИЯ dim_company
INSERT INTO public.dim_company (ticker, company_name, gics_sector)
SELECT DISTINCT 
    TRIM(UPPER(ticker)) AS ticker,
    TRIM(company_name) AS company_name,
    COALESCE(NULLIF(TRIM(gics_sector), ''), 'Unknown') AS gics_sector
FROM staging.raw_financial_feed
WHERE ticker IS NOT NULL
ON CONFLICT (ticker) DO UPDATE 
SET 
    company_name = EXCLUDED.company_name,
    gics_sector = EXCLUDED.gics_sector;

-- 2. СИНХРОНИЗАЦИЯ ИЗМЕРЕНИЯ dim_metric_dictionary
INSERT INTO public.dim_metric_dictionary (metric_code, metric_type)
SELECT DISTINCT 
    TRIM(UPPER(metric_code)) AS metric_code,
    COALESCE(NULLIF(TRIM(UPPER(metric_type)), ''), 'FINANCIAL') AS metric_type
FROM staging.raw_financial_feed
WHERE metric_code IS NOT NULL
ON CONFLICT (metric_code) DO NOTHING;

-- 3. ПАКЕТНАЯ ВСТАВКА В ТАБЛИЦУ ФАКТОВ fact_financial_metrics
-- Разрешение суррогатных ключей выполняется внутри СУБД без участия памяти Python
WITH clean_staging AS (
    SELECT 
        TRIM(report_date)::DATE AS report_date,
        TRIM(UPPER(ticker)) AS ticker,
        TRIM(UPPER(metric_code)) AS metric_code,
        TRIM(metric_value)::NUMERIC(20, 4) AS metric_value
    FROM staging.raw_financial_feed
    WHERE metric_value IS NOT NULL 
      AND metric_value ~ '^-?[0-9]+(\.[0-9]+)?$' -- Регулярное выражение: проверка на число
),
resolved_facts AS (
    SELECT 
        cs.report_date,
        c.company_sk,
        m.metric_id,
        cs.metric_value
    FROM clean_staging cs
    INNER JOIN public.dim_company c ON cs.ticker = c.ticker
    INNER JOIN public.dim_metric_dictionary m ON cs.metric_code = m.metric_code
)
INSERT INTO public.fact_financial_metrics (report_date, company_sk, metric_id, metric_value)
SELECT 
    rf.report_date, 
    rf.company_sk, 
    rf.metric_id, 
    rf.metric_value
FROM resolved_facts rf
WHERE NOT EXISTS ( 
    SELECT 1 
    FROM public.fact_financial_metrics f
    WHERE f.report_date = rf.report_date 
      AND f.company_sk = rf.company_sk 
      AND f.metric_id = rf.metric_id
);

-- 4. ОЧИСТКА БУФЕРА STAGING ПОСЛЕ УСПЕШНОГО ПЕРЕНОСА (DATA HYGIENE)
TRUNCATE TABLE staging.raw_financial_feed;

COMMIT;
"""

with engine.begin() as conn:
    conn.execute(text(elt_transform_sql))
    print("[OK] Стадия TRANSFORM завершена успешно: измерения актуализированы, факты загружены, Staging очищен.")
```

---

### [MARKDOWN ЯЧЕЙКА 5]
```markdown
## ЭТАП 11: РАЗВЕРТЫВАНИЕ АНАЛИТИЧЕСКИХ ВИТРИН (DATA MARTS)

Модель хранения EAV (сущность-атрибут-значение) в таблице `fact_financial_metrics` эффективна для расширения набора метрик, но не приспособлена для построения отчетов в BI и проведения расчетов.

Средствами SQL создается материализованное или классическое представление (VIEW), которое:
* Трансформирует строки метрик в реляционные столбцы через условную фильтрацию (`FILTER (WHERE ...)`).
* Рассчитывает ключевые бизнес-показатели: **EBITDA Margin (%)** и **Net Profit Margin (%)**.
* Рассчитывает квартальную динамику показателей при помощи аналитических оконных функций (`LAG`).
```

---

### [CODE ЯЧЕЙКА 5] Создание витрины финансовых показателей
```python
view_mart_sql = """
CREATE OR REPLACE VIEW public.view_corporate_financial_mart AS
WITH flattened_metrics AS (
    SELECT 
        f.report_date,
        c.ticker,
        c.company_name,
        c.gics_sector,
        -- Агрегация строк EAV в столбцы через SQL FILTER
        MAX(f.metric_value) FILTER (WHERE m.metric_code = 'REVENUE') AS revenue,
        MAX(f.metric_value) FILTER (WHERE m.metric_code = 'EBITDA') AS ebitda,
        MAX(f.metric_value) FILTER (WHERE m.metric_code = 'NET_PROFIT') AS net_profit,
        MAX(f.metric_value) FILTER (WHERE m.metric_code = 'FREE_CASH_FLOW') AS free_cash_flow
    FROM public.fact_financial_metrics f
    JOIN public.dim_company c ON f.company_sk = c.company_sk
    JOIN public.dim_metric_dictionary m ON f.metric_id = m.metric_id
    GROUP BY f.report_date, c.ticker, c.company_name, c.gics_sector
)
SELECT 
    report_date,
    ticker,
    company_name,
    gics_sector,
    revenue,
    ebitda,
    net_profit,
    free_cash_flow,
    -- Расчет маржинальности бизнеса
    ROUND((ebitda / NULLIF(revenue, 0)) * 100, 2) AS ebitda_margin_pct,
    ROUND((net_profit / NULLIF(revenue, 0)) * 100, 2) AS net_profit_margin_pct,
    -- Оконная функция: расчет изменения выручки по отношению к предыдущему зафиксированному периоду
    ROUND(
        revenue - LAG(revenue) OVER(PARTITION BY ticker ORDER BY report_date), 
        2
    ) AS revenue_qoq_change
FROM flattened_metrics;
"""

with engine.begin() as conn:
    conn.execute(text(view_mart_sql))
    print("[OK] Аналитическая витрина public.view_corporate_financial_mart создана.")

# Контрольная выгрузка витрины в Pandas
df_mart = pd.read_sql("SELECT * FROM public.view_corporate_financial_mart ORDER BY ticker, report_date;", engine)
display(df_mart)
```

---

### [MARKDOWN ЯЧЕЙКА 6]
```markdown
## ЭТАП 12: АВТОМАТИЗИРОВАННЫЙ АУДИТ КАЧЕСТВА ДАННЫХ (DATA QUALITY GATES)

Для проверки отсутствия аномалий и повреждения данных после загрузки реализуется набор автоматических проверок (Data Quality Checks).
Критическими инцидентами являются:
* Наличие отрицательной выручки (`revenue < 0`).
* Случаи, когда чистая прибыль превышает выручку (`net_profit > revenue`).
* Записи-сироты (факты, потерявшие связь с измерениями).
```

---

### [CODE ЯЧЕЙКА 6] Запуск валидационных тестов (DQ Check)
```python
dq_query = """
SELECT 
    'Отрицательная выручка' AS violation_type,
    ticker, report_date, revenue::text AS details
FROM public.view_corporate_financial_mart
WHERE revenue < 0

UNION ALL

SELECT 
    'Чистая прибыль превышает выручку' AS violation_type,
    ticker, report_date, FORMAT('NP: %s, REV: %s', net_profit, revenue) AS details
FROM public.view_corporate_financial_mart
WHERE net_profit > revenue AND revenue IS NOT NULL

UNION ALL

SELECT 
    'Битые внешние ключи (Orphan Facts)' AS violation_type,
    'UNKNOWN' AS ticker, report_date, COUNT(*)::text AS details
FROM public.fact_financial_metrics f
LEFT JOIN public.dim_company c ON f.company_sk = c.company_sk
WHERE c.company_sk IS NULL
GROUP BY report_date;
"""

df_dq_issues = pd.read_sql(dq_query, engine)

if df_dq_issues.empty:
    print("==================================================")
    print("✓ DATA QUALITY TEST PASSED: Аномалий в DWH не обнаружено.")
    print("==================================================")
else:
    print("==================================================")
    print("⚠ CRITICAL: Обнаружены нарушения целостности данных!")
    print("==================================================")
    display(df_dq_issues)
```

---

### [MARKDOWN ЯЧЕЙКА 7]
```markdown
## ЭТАП 13: ФОРМИРОВАНИЕ СВЕЖЕГО ЭТАЛОННОГО ДАМПА (PERSISTENCE UPDATE)

Поскольку в базу данных были добавлены новые структуры (`staging`, `view_corporate_financial_mart`) и строки, необходимо обновить эталонный бэкап `/content/elt_dwh_backup.sql`, используемый скриптом **Disaster Recovery** из Этапа 7.
```

---

### [CODE ЯЧЕЙКА 7] Актуализация бэкапа
```python
LOCAL_BACKUP_PATH = '/content/elt_dwh_backup.sql'

# Создание нового эталонного снимка базы
!PGPASSWORD='admin123' pg_dump -h localhost -U dwh_admin -c elt_dwh > {LOCAL_BACKUP_PATH}

backup_size_kb = os.path.getsize(LOCAL_BACKUP_PATH) / 1024
print(f"[OK] Эталонный файл резервной копии обновлен: {LOCAL_BACKUP_PATH} ({backup_size_kb:.2f} KB)")
```

---

### [MARKDOWN ЯЧЕЙКА 8]
```markdown
---
### САМОСТОЯТЕЛЬНАЯ РАБОТА СТУДЕНТА (ЗАДАЧИ НА ДОРАБОТКУ)

1. **Расширение набора метрик:**
   Добавьте в процедуру инжекции метрику `CAPEX` (Capital Expenditures) для тикеров `GAZP` и `LKOH`. Модифицируйте витрину `public.view_corporate_financial_mart`, добавив колонку `capex` и расчет свободного денежного потока по формуле:
   $$\text{Calculated\_FCF} = \text{EBITDA} - \text{CAPEX}$$

2. **Проверка идемпотентности:**
   Выполните повторный запуск **[CODE ЯЧЕЙКИ 3]** и **[CODE ЯЧЕЙКИ 4]**. Убедитесь, что количество записей в таблице `public.fact_financial_metrics` осталось неизменным (отсутствуют дубликаты записей на одинаковые отчетные даты).

3. **Аудит производительности:**
   Выполните в ячейке SQL-команду `EXPLAIN ANALYZE SELECT * FROM public.view_corporate_financial_mart;`. Убедитесь, что для соединения таблиц задействован индекс `idx_fact_fin_metrics`.
```
