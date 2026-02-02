import os
import json
import requests
from datetime import datetime
from typing import Optional

# Конфигурация
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHANNEL_ID = os.environ.get("TELEGRAM_CHANNEL_ID")
DATA_URL = "https://raw.githubusercontent.com/Baskerville42/outage-data-ua/main/data/kyiv.json"
CACHE_FILE = "last_hash.txt"

# Названия дней недели
DAYS_UA = {
    0: "Понеділок",
    1: "Вівторок",
    2: "Середа",
    3: "Четвер",
    4: "П'ятниця",
    5: "Субота",
    6: "Неділя"
}


def format_hours(hours: float) -> str:
    """Склонение слова 'година'"""
    # Убираем .0 для целых чисел
    if hours == int(hours):
        hours = int(hours)
    
    # Для дробных всегда "години"
    if isinstance(hours, float):
        return f"{hours} години"
    
    # Для целых чисел - правильное склонение
    if hours % 10 == 1 and hours % 100 != 11:
        return f"{hours} година"
    elif hours % 10 in [2, 3, 4] and hours % 100 not in [12, 13, 14]:
        return f"{hours} години"
    else:
        return f"{hours} годин"


def format_slot_time(slot: int) -> str:
    """Конвертирует номер слота (0-48) во время"""
    total_minutes = slot * 30
    hours = total_minutes // 60
    minutes = total_minutes % 60
    
    if hours == 24:
        return "24:00"
    
    return f"{hours:02d}:{minutes:02d}"


def fetch_data() -> Optional[dict]:
    """Получаем данные из репозитория"""
    try:
        response = requests.get(DATA_URL, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Ошибка получения данных: {e}")
        return None


def get_cached_hash() -> Optional[str]:
    """Получаем сохраненный хеш"""
    try:
        with open(CACHE_FILE, "r") as f:
            return f.read().strip()
    except FileNotFoundError:
        return None


def save_hash(hash_value: str):
    """Сохраняем хеш"""
    with open(CACHE_FILE, "w") as f:
        f.write(hash_value)


def build_schedule(day_data: dict) -> list[dict]:
    """
    Строим расписание с получасовыми интервалами.
    
    Значения:
    - "yes" = свет есть весь час
    - "no" = света нет весь час  
    - "first" = света нет ПЕРВЫЕ 30 минут часа
    - "second" = света нет ВТОРЫЕ 30 минут часа
    - "maybe" = возможно отключение (считаем как свет есть)
    """
    # Создаём массив из 48 получасовых слотов
    slots = []
    
    for hour in range(1, 25):
        hour_key = str(hour)
        status = day_data.get(hour_key, "yes")
        
        if status == "yes":
            first_half = True
            second_half = True
        elif status == "no":
            first_half = False
            second_half = False
        elif status == "first":
            # Света НЕТ первые 30 минут
            first_half = False
            second_half = True
        elif status == "second":
            # Света НЕТ вторые 30 минут
            first_half = True
            second_half = False
        elif status in ["maybe", "mfirst", "msecond"]:
            first_half = True
            second_half = True
        else:
            first_half = True
            second_half = True
        
        slots.append(first_half)
        slots.append(second_half)
    
    # Объединяем последовательные слоты с одинаковым статусом
    if not slots:
        return []
    
    periods = []
    current_status = slots[0]
    start_slot = 0
    
    for i in range(1, len(slots)):
        if slots[i] != current_status:
            hours = (i - start_slot) * 0.5
            periods.append({
                "start": format_slot_time(start_slot),
                "end": format_slot_time(i),
                "is_on": current_status,
                "hours": hours
            })
            current_status = slots[i]
            start_slot = i
    
    # Добавляем последний период
    hours = (len(slots) - start_slot) * 0.5
    periods.append({
        "start": format_slot_time(start_slot),
        "end": format_slot_time(len(slots)),
        "is_on": current_status,
        "hours": hours
    })
    
    return periods


def format_schedule_message(schedule: list[dict], date: datetime) -> str:
    """Форматируем сообщение для одного дня"""
    day_name = DAYS_UA[date.weekday()]
    date_str = date.strftime("%d.%m")
    
    lines = [f"🗓 Графік відключень на {date_str} ({day_name}):"]
    lines.append("")  # Пустая строка после заголовка
    
    total_on = 0.0
    total_off = 0.0
    
    for period in schedule:
        emoji = "🔋" if period["is_on"] else "🪫"
        hours_text = format_hours(period["hours"])
        
        if period["is_on"]:
            # Формат: (4 години Світло є)
            status_text = f"({hours_text} Світло є)"
        else:
            # Формат: (Світла нема 7 годин)
            status_text = f"(Світла нема {hours_text})"
        
        lines.append(f"{emoji}{period['start']} - {period['end']} {status_text}")
        
        if period["is_on"]:
            total_on += period["hours"]
        else:
            total_off += period["hours"]
    
    lines.append("")
    lines.append(f"Світло є {format_hours(total_on)}")
    lines.append(f"Світла нема {format_hours(total_off)}")
    
    return "\n".join(lines)


def format_full_message(data: dict) -> Optional[str]:
    """Формируем полное сообщение для всех групп и дней"""
    fact_data = data.get("fact", {}).get("data", {})
    
    if not fact_data:
        return None
    
    # Сортируем дни по timestamp
    sorted_days = sorted(fact_data.keys(), key=lambda x: int(x))
    
    groups = ["GPV12.1", "GPV18.1"]
    all_group_messages = []
    
    for group in groups:
        group_num = group.replace("GPV", "")
        header = f"============ група {group_num} ============"
        
        day_messages = []
        for day_ts in sorted_days[:2]:  # только два дня
            day_data = fact_data[day_ts].get(group)
            if not day_data:
                continue
            
            # Конвертируем timestamp в дату
            date = datetime.fromtimestamp(int(day_ts))
            schedule = build_schedule(day_data)
            message = format_schedule_message(schedule, date)
            day_messages.append(message)
        
        if day_messages:
            # Соединяем дни разделителем
            days_text = "\n\n-------------------------------------\n".join(day_messages)
            all_group_messages.append(f"{header}\n{days_text}")
    
    # Соединяем группы двумя пустыми строками
    return "\n\n\n".join(all_group_messages)


def send_telegram_message(message: str) -> bool:
    """Отправляем сообщение в Telegram"""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHANNEL_ID:
        print("Telegram credentials not configured")
        return False
    
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    
    # Разбиваем на части, если сообщение слишком длинное
    max_length = 4000
    
    if len(message) <= max_length:
        parts = [message]
    else:
        # Разбиваем по группам
        parts = message.split("\n\n\n")
    
    for part in parts:
        payload = {
            "chat_id": TELEGRAM_CHANNEL_ID,
            "text": part,
            "parse_mode": "HTML"
        }
        
        try:
            response = requests.post(url, json=payload, timeout=30)
            response.raise_for_status()
            print("Сообщення відправлено успішно")
        except Exception as e:
            print(f"Помилка відправки: {e}")
            return False
    
    return True


def main():
    print("Fetching data...")
    data = fetch_data()
    
    if not data:
        print("Failed to fetch data")
        return
    
    # Проверяем, есть ли обновления
    content_hash = data.get("meta", {}).get("contentHash", "")
    cached_hash = get_cached_hash()
    
    if content_hash == cached_hash:
        print("No updates detected")
        return
    
    print(f"New data detected! Hash: {content_hash[:16]}...")
    
    # Форматируем сообщение
    message = format_full_message(data)
    
    if not message:
        print("Failed to format message")
        return
    
    print("Generated message:")
    print("-" * 50)
    print(message)
    print("-" * 50)
    
    # Отправляем в Telegram
    if send_telegram_message(message):
        save_hash(content_hash)
        print("Hash saved")
    else:
        print("Failed to send message, hash not saved")


if __name__ == "__main__":
    main()
