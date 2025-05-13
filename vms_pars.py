import gspread
import requests
import json
from datetime import datetime
import uuid
from oauth2client.service_account import ServiceAccountCredentials

# Настройки
GOOGLE_CREDS = "credentials.json"  # Путь к файлу с ключами от Google
SPREADSHEET_ID = "1UDGVsRM_IekTKjCgFWRmo8W0CiRiA0KNUO2lTJQIcW0"  # ID таблицы
YC_IAM_TOKEN = "t1.9euelZqWyI_NjY3Ox5vIm5yZyImZzO3rnpWal5iTjcqOlpednpqOkJPLnYnl8_dlfUFA-e9NAxEK_t3z9yUsP0D5700DEQr-zef1656VmsudmJubm8uKx8abkY-YmJfN7_zN5_XrnpWak8aens2bk5aNlM_OlsaWi5Dv_cXrnpWay52Ym5uby4rHxpuRj5iYl80.YrPSOOK8IO-feo3ZBziBpDDeog69fY7PNp_vg9lvFSYBMK4FKEKBiWfKCVpwtIeiXOHqP8uq_I72v_jxQBSgDg"
FOLDER_ID = "b1guh54hstiecdkcat8s"  # ← твой folder-id
QUEUE_NAME = "dev-mq.fifo"  # Имя очереди
YC_QUEUE_URL = "https://message-queue.api.cloud.yandex.net/b1g0ca3i23kc5sdjfj23/dj6000000011jruh0068/dev-mq.fifo"


# 1. Парсинг Google Sheets
def parse_sheets():
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
        return []

    data_to_send = []
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for i, row in enumerate(rows[1:], start=2):
        if len(row) > ready_col and str(row[ready_col]).strip().upper() == "TRUE":
            row_id = row[id_col] if len(row) > id_col else ""

            # Считаем, что строка новая, если ID пустой
            is_new = not row_id.strip()

            if is_new:
                row_id = str(uuid.uuid4())
                sheet.update_cell(i, id_col + 1, row_id)

            # Обновляем дату в колонке L (12-я колонка)
            sheet.update_cell(i, 12, now_str)

            # Собираем данные в словарь
            row_data = dict(zip(headers, row))
            row_data["ID"] = row_id
            row_data["event_type"] = "create" if is_new else "update"

            data_to_send.append(row_data)

    return data_to_send
# 2. Отправка в YMQ
def send_to_ymq(data):
    messages = [
        {
            "body": json.dumps(row),
            "message_group_id": "default"
        } for row in data
    ]

    response = requests.post(
        f"{YC_QUEUE_URL}/messages",
        headers={
            "Authorization": f"Bearer {YC_IAM_TOKEN}",
            "Content-Type": "application/json"
        },
        json={"messages": messages}
    )

    print("📦 Код ответа:", response.status_code)
    print("📩 Ответ:", response.text)
    return response.status_code == 200

# 3. Главная функция
if __name__ == "__main__":
    data = parse_sheets()
    if not data:
        print("🤷 Нет данных для отправки")
    elif send_to_ymq(data):
        print(f"✅ Успешно отправлено {len(data)} строк в очередь!")
    else:
        print("❌ Ошибка при отправке")
