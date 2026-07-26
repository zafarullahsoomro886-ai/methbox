# Zedox Telegram Bot

A Telegram bot with force-join, free/VIP methods, services and digital products, referrals, balances, multiple admins, owner-controlled admin permissions, broadcasts, auto-posting and auto-import/upload tools.

## Required Railway variables

Open the Railway service, then **Variables**, and add:

```env
BOT_TOKEN=your_new_bot_token
ADMIN_ID=your_numeric_personal_telegram_id
MONGO_URI=your_mongodb_connection_string
BOT_WORKERS=12
```

Do not add quotes, spaces, `@username`, channel IDs or group IDs to `ADMIN_ID`. It must contain digits only.

## GitHub deployment

1. Upload all repository files to GitHub.
2. Do not upload a `.env` file.
3. Create a Railway project and select **Deploy from GitHub repo**.
4. Add the required variables in Railway.
5. Deploy or restart the service.

## MongoDB Atlas

In Atlas, open **Network Access** and add an IP rule suitable for your host. Railway commonly requires `0.0.0.0/0`; use a strong database password and least-privilege database user.

## VIP contact

The VIP button does not charge points. It opens the Telegram username or link configured by the owner:

`Admin Panel → Set Contacts → VIP Contact`

Accepted formats include `@username`, `username`, or a Telegram link.

## Admin permissions

Only the owner can add/remove admins or change their permissions:

`Admin Panel → Admin Management → Permissions`

Permission restrictions are checked by the server, including callback actions.

## Local start

```bash
python -m venv .venv
source .venv/bin/activate       # Linux/macOS
# .venv\\Scripts\\activate      # Windows
pip install -r requirements.txt
python bot.py
```
