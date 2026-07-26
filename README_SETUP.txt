ZEDOX TELEGRAM BOT - SETUP

Required environment variables:
BOT_TOKEN=your_new_bot_token
MONGO_URI=your_mongodb_connection_string
ADMIN_ID=your_numeric_telegram_user_id

Optional:
BOT_WORKERS=12

Install:
pip install -r requirements.txt

Run:
python bot.py

OWNER FEATURES
1. Open the bot and send /start.
2. Open ADMIN PANEL > Set Contacts > VIP Contact.
3. Enter the Telegram username that the Buy VIP button should open, for example @yourusername.
4. Open ADMIN PANEL > Admin Management.
5. Add an admin using their numeric Telegram ID.
6. Open Permissions and enable/disable each permission group.

SECURITY
- Never place BOT_TOKEN or MONGO_URI directly inside bot.py.
- The owner cannot be removed.
- Admin permission changes apply immediately.
