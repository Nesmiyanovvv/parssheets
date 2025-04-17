import jwt
import time
import requests
from cryptography.hazmat.primitives import serialization

# Данные сервисного аккаунта
SERVICE_ACCOUNT_ID = "ajehglr5qihbaeqol4bv"
KEY_ID = "ajel9aa2dlirk01i9ito"
PRIVATE_KEY = """-----BEGIN PRIVATE KEY-----
приватный ключ
-----END PRIVATE KEY-----"""

# 1. Проверка ключа (опционально)
try:
    serialization.load_pem_private_key(PRIVATE_KEY.encode(), password=None)
    print("✅ Приватный ключ валиден")
except Exception as e:
    print(f"❌ Ошибка в ключе: {e}")
    exit()

# 2. Генерация JWT-токена
now = int(time.time())
jwt_payload = {
    "aud": "https://iam.api.cloud.yandex.net/iam/v1/tokens",  # URL должен быть точным
    "iss": "ajehglr5qihbaeqol4bv",  # ID сервисного аккаунта
    "iat": int(time.time()),  # Текущее время в секундах
    "exp": int(time.time()) + 3600  # Через 1 час
}

jwt_token = jwt.encode(
    jwt_payload,
    PRIVATE_KEY,
    algorithm="PS256",
    headers={"kid": KEY_ID}
)

# 3. Получение IAM-токена
response = requests.post(
    "https://iam.api.cloud.yandex.net/iam/v1/tokens",
    json={"jwt": jwt_token},
    timeout=10
)

if response.status_code == 200:
    iam_token = response.json()["iamToken"]
    print(f"✅ IAM-токен получен (действует 1 час):\n{iam_token}")
else:
    print(f"❌ Ошибка HTTP {response.status_code}: {response.text}")