import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://api.gripp.com/public/api3.php"
GRIPP_API_KEY = os.getenv("GRIPP_API_KEY")
HEADERS = {"Authorization": f"Bearer {GRIPP_API_KEY}"}

INVOICE_ID = 2016  # <-- Vul hier een bestaand factuur-ID in (int of str)

def get_invoice(invoice_id):
    payload = [
        {
            "method": "invoice.getone",
            "params": [
                [
                    {"field": "invoice.id", "operator": "equals", "value": invoice_id}
                ]
            ],
            "id": 1
        }
    ]
    response = requests.post(BASE_URL, headers=HEADERS, json=payload)
    response.raise_for_status()
    data = response.json()
    return data

def main():
    invoice_id = INVOICE_ID
    data = get_invoice(invoice_id)
    print(json.dumps(data, indent=2))
    # Zoek subject en invoicelines in de response
    try:
        rows = data[0]["result"].get("rows", [])
        if rows:
            invoice = rows[0]
            print("Subject:", invoice.get("subject"))
            print("Invoicelines:", invoice.get("invoicelines"))
        else:
            print("Geen factuur gevonden met het opgegeven ID.")
    except Exception as e:
        print("Kon subject of invoicelines niet vinden in de response:", e)

if __name__ == "__main__":
    main() 