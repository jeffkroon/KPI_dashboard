# Veilige rapportageverzending via n8n en Streamlit

Deze handleiding beschrijft hoe je veilig rapportages verstuurt vanuit je Streamlit-dashboard via een n8n-webhook, met focus op beveiliging, whitelisting en autorisatie.

---

## 1. **Doel**
- Rapportages worden vanuit de backend (Streamlit/Python) als JSON naar een n8n-webhook gestuurd.
- n8n verzorgt het daadwerkelijke versturen van e-mails (bijv. via Outlook, SMTP, etc.).
- Alleen geautoriseerde gebruikers en adressen mogen rapportages versturen.

---

## 2. **Hoe werkt de token?**
- De token is een geheime, unieke string (bijv. `JOUW_SUPERGEHEIME_TOKEN_123456`).
- De Python-app stuurt deze token mee in de `Authorization`-header van elke POST naar de n8n-webhook:
  ```
  Authorization: Bearer JOUW_SUPERGEHEIME_TOKEN_123456
  ```
- De token wordt als environment variable (`N8N_WEBHOOK_TOKEN`) op de backend opgeslagen, niet in de frontend.
- In n8n controleer je deze token direct na de Webhook node.

---

## 3. **Whitelisting en autorisatie in n8n**

### **a) Authorization check**
Voeg direct na de Webhook node een Function node toe:
```javascript
const auth = $headers["authorization"];
if (auth !== "Bearer JOUW_SUPERGEHEIME_TOKEN_123456") {
  throw new Error("Unauthorized");
}
return items;
```
> **Best practice:** Zet de token als environment variable in n8n, of gebruik een n8n-credential, zodat je hem niet hardcodeert.

### **b) Whitelisting van ontvangers**
Voeg na de authenticatie een Function node toe:
```javascript
const allowedDomains = ['bedrijf.nl'];
const to = $json["to"];
if (!allowedDomains.some(domain => to.endsWith('@' + domain))) {
  throw new Error('Ongeldig e-mailadres');
}
return items;
```
> Je kunt ook een lijst van specifieke adressen whitelisten.

---

## 4. **Security best practices**
- Gebruik altijd HTTPS voor je n8n-server.
- Zet de token als environment variable, niet in de code.
- Gebruik authenticatie in je Streamlit-dashboard (bijv. streamlit-authenticator).
- Toon rapportagefunctionaliteit alleen aan geauthenticeerde gebruikers.
- Log alle verzendpogingen in n8n (bijv. naar een database of Google Sheet).
- Voeg rate limiting toe in n8n om abuse te voorkomen.
- Monitor en alarmeer bij verdachte pogingen.

---

## 5. **Voorbeeld n8n-workflow**
1. **Webhook node** (ontvangt POST van Python-app)
2. **Function node** (controleert Authorization-header)
3. **Function node** (controleert of het e-mailadres is toegestaan)
4. **E-mail node** (stuurt de mail via Outlook/SMTP)
5. **(Optioneel) Logging node** (logt verzending)

---

## 6. **Beste authorization-methode**
- Gebruik een **Bearer-token** in de `Authorization`-header.
- Dit is veilig, makkelijk te beheren en standaard ondersteund in n8n en Python.
- Vervang de token direct als je vermoedt dat hij gelekt is.

---

## 7. **Samenvatting stappen**
1. Genereer een geheime token en zet deze als environment variable op je backend en in n8n.
2. Laat je Python-app de token meesturen in de Authorization-header.
3. Controleer de token in n8n vóórdat je iets met de data doet.
4. Controleer in n8n of het e-mailadres is toegestaan (whitelist).
5. Verstuur alleen dan de e-mail.
6. Log en monitor alle verzendingen.

---

## 8. **Brainstorm: Verdere automatisering en integratie met n8n**

Hieronder vind je ideeën voor verdere uitbreiding van je workflow met n8n:

- **Notificaties & Alerts**
  - Stuur automatisch een bericht naar Slack, Teams, Discord of SMS bij belangrijke rapportages of fouten.
  - Push-notificaties bij urgente meldingen (bijv. via Pushover of Twilio).

- **Logging & Auditing**
  - Sla alle verzonden rapportages op in een database, Google Sheet of Excel voor auditing/compliance.
  - Log verzendingen in een gedeeld spreadsheet voor transparantie.

- **Goedkeuringsflows**
  - Laat een manager eerst goedkeuren voordat een rapportage daadwerkelijk wordt verstuurd (bijv. via e-mail of Slack-reactie).
  - Multi-step approval: meerdere mensen moeten akkoord geven voordat de mail eruit mag.

- **Data-verrijking & -validatie**
  - Controleer of de rapportage aan bepaalde eisen voldoet (bijv. geen gevoelige data, juiste format).
  - Voeg automatisch extra info toe aan de rapportage (bijv. bedrijfslogo, extra statistieken).

- **Archivering**
  - Archiveer elke rapportage automatisch in SharePoint, Google Drive, S3, etc.
  - Genereer automatisch een PDF van de rapportage en sla die op of verstuur als bijlage.

- **Incident Management**
  - Maak automatisch een ticket aan in Jira, ServiceNow, Asana, etc. bij bepaalde rapportages of fouten.

- **Rate Limiting & Abuse Detection**
  - Blokkeer gebruikers/IP's die te vaak rapportages proberen te versturen.
  - Stuur een alert naar de beheerder bij verdachte activiteit.

- **Integratie met andere systemen**
  - Koppel rapportages aan klantdossiers of projecten in een CRM/ERP.
  - Trigger andere API's op basis van rapportage-inhoud.

> **Tip:** Begin simpel en veilig. Breid uit als je meer controle, logging of integratie nodig hebt.

---

**Vragen of hulp nodig? Neem contact op met je beheerder of security officer.** 