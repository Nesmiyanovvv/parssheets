import jwt
import time
import requests
from cryptography.hazmat.primitives import serialization

# Константы для генерации токена
SERVICE_ACCOUNT_ID = "ajehglr5qihbaeqol4bv"
KEY_ID = "ajel9aa2dlirk01i9ito"
PRIVATE_KEY = """-----BEGIN PRIVATE KEY-----
приватный ключ
-----END PRIVATE KEY-----"""

# Хранилище токена и времени
_iam_token = None
_iam_token_expire = 0

def get_iam_token():
    global _iam_token, _iam_token_expire

    # Если токен ещё действителен, возвращаем его
    if _iam_token and time.time() < _iam_token_expire - 60:
        return _iam_token

    try:
        # Проверка валидности ключа
        serialization.load_pem_private_key(PRIVATE_KEY.encode(), password=None)
    except Exception as e:
        raise RuntimeError(f"❌ Проблема с приватным ключом: {e}")

    now = int(time.time())
    jwt_payload = {
        "aud": "https://iam.api.cloud.yandex.net/iam/v1/tokens",
        "iss": SERVICE_ACCOUNT_ID,
        "iat": now,
        "exp": now + 3600 * 12  # JWT живёт 12 часов
    }

    encoded_jwt = jwt.encode(
        jwt_payload,
        PRIVATE_KEY,
        algorithm="PS256",
        headers={"kid": KEY_ID}
    )

    response = requests.post(
        "https://iam.api.cloud.yandex.net/iam/v1/tokens",
        json={"jwt": encoded_jwt},
        timeout=10
    )

    if response.status_code == 200:
        data = response.json()
        _iam_token = data["iamToken"]
        _iam_token_expire = now + 3600 * 12
        print("🔐 Новый IAM-токен получен.")
        return _iam_token
    else:
        raise RuntimeError(f"❌ Ошибка при получении IAM-токена: {response.status_code}, {response.text}")
