import sys
import rapidjson as json
import optional_faker as _
import uuid
import random

from dotenv import load_dotenv
from datetime import date, datetime
from faker import Faker

load_dotenv()
fake = Faker()
inventory = [
    "Peugeot 208", "Peugeot 3008", "Citroen C3", "Renault Megane",
    "Fiat 500e", "Maserati Grecale Folgore", "Renault Mégane E-Tech",
    "Peugeot e-208", "Peugeot e-3008", "Citroen ë-C4", "Citroen Ami",
    "DS 3 E-Tense", "DS 4 E-Tense", "DS 7 E-Tense", "DS 9 E-Tense",
    "Fiat 600e", "Jeep Avenger EV", "Opel Mokka-e", "Opel Corsa-e",
    "Opel Astra Electric", "Peugeot e-2008", "Citroen ë-Berlingo",
    "Fiat E-Ulysse", "Peugeot e-Rifter", "Jeep Recon EV", "Jeep Wagoneer S",
    "Maserati GranTurismo Folgore", "Maserati MC20 Folgore", "Opel Zafira-e Life"
]

global car_descriptions
with open('car_descriptions.json', 'r', encoding="utf-8") as f:
    car_descriptions = json.load(f)

for car in car_descriptions:
    car["buy_price"] = int(car["prix_estime"] * random.uniform(0.8, 0.95))



def print_client_support():
    global inventory
    car_model = fake.random_element(elements=inventory)


    car_horsepower = next((car["horsepower"] for car in car_descriptions if car["name"] == car_model), None)
    car_engine = next((car["engine"] for car in car_descriptions if car["name"] == car_model), None)
    car_brand = next((car["brand"] for car in car_descriptions if car["name"] == car_model), None)
    # random -5 to -20% from sell_price reduction
    buy_price = next((car["buy_price"] for car in car_descriptions if car["name"] == car_model), None)
    car_type = next((car["type"] for car in car_descriptions if car["name"] == car_model), None)
    car_autonomy = next((car["autonomy"] for car in car_descriptions if car["name"] == car_model), None)
    car_consumption = next((car["consumption"]["kWh_100km"] for car in car_descriptions if car["name"] == car_model), None)
    car_release_date = next((car["release_date"] for car in car_descriptions if car["name"] == car_model), None)



    client_support = {'txid': str(uuid.uuid4()),
                    'car_model': car_model,
                    'brand': car_brand,
                    'engine': car_engine,
                    'horsepower': car_horsepower,
                    'buy_price': buy_price,
                    'type' : car_type,
                    'autonomy' : car_autonomy,
                    'consumption' : car_consumption,
                    'release_date' : car_release_date,
    }
    d = json.dumps(client_support) + '\n'
    sys.stdout.write(d)


if __name__ == "__main__":
    args = sys.argv[1:]
    total_count = int(args[0])
    for _ in range(total_count):
        print_client_support()
    print('')