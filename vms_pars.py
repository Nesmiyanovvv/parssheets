import gspread
import psycopg2
import requests
import json
import time
import uuid
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
from iam import get_iam_token


# Настройка подключения к БД
DB_CONFIG = {
    "dbname": "db_name",
    "user": "user",
    "password": "password",
    "host": "localhost",
    "port": 5432
}

# Настройки
GOOGLE_CREDS = "credentials.json"
SPREADSHEET_ID = "1UDGVsRM_IekTKjCgFWRmo8W0CiRiA0KNUO2lTJQIcW0"
YC_IAM_TOKEN = "твой_токен"
QUEUE_NAME = "dev-mq.fifo"
YC_QUEUE_URL = "https://message-queue.api.cloud.yandex.net/b1g0ca3i23kc5sdjfj23/dj6000000011jruh0068/dev-mq.fifo"

# Глобальное хранилище данных для отслеживания изменений
last_known_state = {}


def get_sheet_data():
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        GOOGLE_CREDS, ["https://www.googleapis.com/auth/spreadsheets"]
    )
    client = gspread.authorize(creds)
    sheet = client.open_by_key(SPREADSHEET_ID).sheet1

    rows = sheet.get_all_values(value_render_option='UNFORMATTED_VALUE')
    headers = rows[0]

    ready_col = headers.index("Готово") if "Готово" in headers else None
    id_col = headers.index("ID") if "ID" in headers else None

    if ready_col is None or id_col is None:
        print("❌ Столбец 'Готово' или 'ID' не найден")
        return [], headers, sheet, ready_col, id_col

    data = []
    for i, row in enumerate(rows[1:], start=2):
        if len(row) > ready_col and str(row[ready_col]).strip().upper() == "TRUE":
            data.append((i, dict(zip(headers, row))))
    return data, headers, sheet, ready_col, id_col


def detect_changes(data, id_col, headers, sheet):
    global last_known_state
    updated_data = []
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for row_index, row_dict in data:
        row_id = row_dict.get("ID", "").strip()

        if not row_id:
            row_id = str(uuid.uuid4())
            sheet.update_cell(row_index, id_col + 1, row_id)
            row_dict["ID"] = row_id
            row_dict["event_type"] = "create"
        else:
            last = last_known_state.get(row_id)
            current = tuple(row_dict.get(h, "") for h in headers)

            if last == current:
                continue  # Пропускаем, если изменений нет

            row_dict["event_type"] = "update"

        # Обновляем дату в колонке L
        sheet.update_cell(row_index, 12, now_str)

        current_state = tuple(row_dict.get(h, "") for h in headers)
        last_known_state[row_id] = current_state
        updated_data.append(row_dict)

    return updated_data


def send_to_ymq(data):
    messages = [
        {
            "body": json.dumps(row),
            "message_group_id": "default"
        } for row in data
    ]

    token = get_iam_token()

    response = requests.post(
        f"{YC_QUEUE_URL}/messages",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        },
        json={"messages": messages}
    )

    print("📦 Код ответа:", response.status_code)
    print("📩 Ответ:", response.text)
    return response.status_code == 200


def save_to_postgres(data):
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        for row in data:
            cursor.execute("""
                INSERT INTO vms_data (
                    id, zakaz, importer, client, status, container_number,
                    container_size, cargo, waybill_number, client_order_number, file_url
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE
                SET zakaz = EXCLUDED.zakaz,
                    importer = EXCLUDED.importer,
                    client = EXCLUDED.client,
                    status = EXCLUDED.status,
                    container_number = EXCLUDED.container_number,
                    container_size = EXCLUDED.container_size,
                    cargo = EXCLUDED.cargo,
                    waybill_number = EXCLUDED.waybill_number,
                    client_order_number = EXCLUDED.client_order_number,
                    file_url = EXCLUDED.file_url;
            """, (
                row.get("ID"),
                row.get("Заказ"),
                row.get("Импортёр"),
                row.get("Клиент"),
                row.get("Статус"),
                row.get("№ Контейнера"),
                row.get("Типоразмер контейнера"),
                row.get("Груз"),
                row.get("№ накладной"),
                row.get("№ Заказа клиента"),
                row.get("Загрузить"),
            ))

        conn.commit()
        print(f"🗄️ В базу данных добавлено или обновлено {len(data)} строк.")

    except Exception as e:
        print(f"❌ Ошибка при сохранении в БД: {e}")

    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


def main_loop():
    print("🚀 Запуск парсера. Проверка каждые 5 минут.")
    while True:
        try:
            data, headers, sheet, ready_col, id_col = get_sheet_data()
            if not data:
                print("🤷 Нет строк с отметкой 'Готово'")
            else:
                updated = detect_changes(data, id_col, headers, sheet)
                if updated:
                    if send_to_ymq(updated):
                        print(f"✅ Отправлено {len(updated)} строк.")
                        save_to_postgres(updated)
                    else:
                        print("❌ Ошибка при отправке.")
                else:
                    print("🔁 Нет изменений.")
        except Exception as e:
            print(f"💥 Ошибка: {e}")

        time.sleep(300)  # 5 минут


if __name__ == "__main__":
    main_loop()
