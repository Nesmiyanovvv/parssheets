import gspread
import json
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime

SCOPES = ["https://www.googleapis.com/auth/spreadsheets",
          "https://www.googleapis.com/auth/drive"]
SERVICE_ACCOUNT_FILE = "credentials.json"
SPREADSHEET_ID = "1UDGVsRM_IekTKjCgFWRmo8W0CiRiA0KNUO2lTJQIcW0"

credentials = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, SCOPES)
client = gspread.authorize(credentials)

try:
    spreadsheet = client.open_by_key(SPREADSHEET_ID)
except Exception as e:
    print(f"Ошибка при открытии таблицы: {e}")
    exit()

data = {}

def convert_to_serializable(obj):
    if pd.isna(obj) or obj == '':
        return None
    elif isinstance(obj, (pd.Timestamp, datetime)):
        return obj.isoformat()
    elif isinstance(obj, (bool, int, float, str)):
        return obj
    else:
        return str(obj)

for sheet in spreadsheet.worksheets():
    try:
        rows = sheet.get_all_values()

        if not rows:
            print(f"Лист '{sheet.title}' пуст, пропускаем")
            continue

        headers = rows[0]

        if len(rows) == 1:
            print(f"Лист '{sheet.title}' содержит только заголовки")
            data[sheet.title] = []
            continue

        df = pd.DataFrame(rows[1:], columns=headers)

        for col in df.columns:
            df[col] = df[col].apply(lambda x: True if str(x).strip() == '✔' else x)

            df[col] = df[col].replace('', None)

            if col.lower() in ['№ заказа клиента', 'номер заказа']:
                df[col] = df[col].astype(str).str.strip()
                continue

            try:
                if not df[col].empty and df[col].astype(str).str.match(r'^-?\d+\.?\d*$').all():
                    df[col] = pd.to_numeric(df[col])
            except Exception:
                pass

            try:
                if not df[col].empty and any(df[col].astype(str).str.match(r'\d{1,2}[./-]\d{1,2}[./-]\d{2,4}')):
                    df[col] = pd.to_datetime(df[col], format='mixed')
            except Exception:
                pass

        sheet_data = []
        for record in df.to_dict(orient='records'):
            sheet_data.append({
                k: convert_to_serializable(v)
                for k, v in record.items()
            })

        data[sheet.title] = sheet_data

    except Exception as e:
        print(f"Ошибка при обработке листа '{sheet.title}': {e}")
        continue

output_file = "spreadsheet_data.json"
try:
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    print(f"Данные успешно сохранены в {output_file}")
except Exception as e:
    print(f"Ошибка при сохранении файла: {e}")