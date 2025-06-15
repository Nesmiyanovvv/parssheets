таблица  https://docs.google.com/spreadsheets/d/1UDGVsRM_IekTKjCgFWRmo8W0CiRiA0KNUO2lTJQIcW0/edit?usp=sharing

# 📦 Автоматизация логистики для Lamart

Этот проект предназначен для автоматизации логистических бизнес-процессов в компании **Lamart**. Решение построено на модульной архитектуре с использованием Google Sheets, Python и Yandex Message Queue (YMQ).

## 🚀 Цель проекта

Снизить количество ручного труда, минимизировать ошибки и упростить работу сотрудников с логистической информацией.

---

## ⚙️ Основной функционал

- 📊 **Google Таблицы** как интерфейс для менеджеров  
- 🔄 **Парсинг и обработка данных** из таблиц на Python (`gspread`)
- 🔐 **Авторизация через Yandex IAM Token**
- 📬 **Передача данных в CRM/ERP** через **Yandex Message Queue (FIFO)**  
- 📌 **Визуальные скрипты для сотрудников**:
  - Выпадающие списки
  - Чекбоксы
  - Календарь выбора даты
  - Прикрепление файлов в ячейки

---

## 🧰 Технологии

- Python 3.11+
  - `gspread`
  - `requests`
- JavaScript (Google Apps Script)
- Google Sheets
- Yandex Cloud (IAM, YMQ)
- PostgreSQL (для хранения и интеграции)

---
