# =========================
# ZEDOX BOT - COMPLETE VERSION
# With Working Subfolders, Fast Response, Fixed Give Points
# =========================

import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup
import os, time, random, string, threading, hashlib, hmac, json, csv, io, zipfile, traceback, logging, re, unicodedata
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode, quote, unquote
from pymongo import MongoClient, ReturnDocument, WriteConcern
from pymongo.errors import PyMongoError, AutoReconnect, ConnectionFailure, ConfigurationError, OperationFailure
from datetime import datetime, timedelta
from functools import wraps

BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
ADMIN_ID_RAW = os.environ.get("ADMIN_ID", "").strip()
MONGO_URI = os.environ.get("MONGO_URI", "").strip()

def _strip_invisible(value):
    return "".join(ch for ch in str(value) if unicodedata.category(ch) not in ("Cf", "Cc")).strip()

def sanitize_mongo_uri(uri):
    """Remove invisible characters and unsafe URI options."""
    try:
        uri = _strip_invisible(uri)
        parts = urlsplit(uri)
        # Percent-encode the username/password so special characters in the
        # password (@ : / # ? % etc.) don't break authentication. Normalizing
        # via unquote()->quote() is idempotent, so an already-encoded value is
        # left unchanged rather than double-encoded. This is the most common
        # cause of Atlas "bad auth" errors.
        netloc = parts.netloc
        if "@" in netloc:
            userinfo, host = netloc.rsplit("@", 1)
            if ":" in userinfo:
                user, pwd = userinfo.split(":", 1)
                userinfo = quote(unquote(user), safe="") + ":" + quote(unquote(pwd), safe="")
            else:
                userinfo = quote(unquote(userinfo), safe="")
            netloc = userinfo + "@" + host
        parts = parts._replace(netloc=netloc)
        allowed = {
            "retrywrites", "journal", "readpreference", "replicaset", "authsource",
            "tls", "ssl", "tlsallowinvalidcertificates", "connecttimeoutms",
            "sockettimeoutms", "serverselectiontimeoutms", "maxpoolsize",
            "minpoolsize", "appname", "directconnection", "compressors",
            "zlibcompressionlevel", "uuidrepresentation"
        }
        cleaned = []
        for k, v in parse_qsl(parts.query, keep_blank_values=True):
            k = _strip_invisible(k)
            v = _strip_invisible(v)
            if k.lower() in allowed:
                cleaned.append((k, v))
        return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(cleaned), parts.fragment))
    except Exception:
        return _strip_invisible(uri)

MONGO_URI = sanitize_mongo_uri(MONGO_URI)

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN environment variable is missing")
if not ADMIN_ID_RAW or not ADMIN_ID_RAW.isdigit():
    raise RuntimeError("ADMIN_ID must contain digits only")
if not MONGO_URI:
    raise RuntimeError("MONGO_URI environment variable is missing")

ADMIN_ID = int(ADMIN_ID_RAW)

# =========================
# 🌐 MONGODB SETUP (RELIABLE)
# =========================
def connect_mongodb():
    last_error = None
    for attempt in range(1, 11):
        try:
            mongo_client = MongoClient(
                MONGO_URI,
                serverSelectionTimeoutMS=15000,
                connectTimeoutMS=15000,
                socketTimeoutMS=30000,
                maxPoolSize=100,
                minPoolSize=2,
                appname="zedox-bot",
                retryWrites=True,
                retryReads=True,
            )
            mongo_client.admin.command("ping")
            print("✅ MongoDB connected")
            return mongo_client
        except OperationFailure as exc:
            # Authentication/authorization errors are permanent — retrying wastes
            # time and floods the logs. Fail fast with an actionable message.
            print(
                "❌ MongoDB authentication failed (bad auth). This is a credential "
                "problem, not a bug. Check that MONGO_URI has the correct database "
                "username and password, that any special characters in the password "
                "are percent-encoded (@ -> %40, # -> %23, etc.), that the database "
                "user exists in MongoDB Atlas, and that Network Access allows this "
                f"host (0.0.0.0/0). Details: {exc}",
                flush=True,
            )
            raise RuntimeError(f"MongoDB authentication failed: {exc}") from exc
        except Exception as exc:
            last_error = exc
            print(f"⚠️ MongoDB connection attempt {attempt}/10 failed: {exc}", flush=True)
            time.sleep(min(attempt * 3, 20))
    raise RuntimeError(f"MongoDB connection failed after retries: {last_error}")

client = connect_mongodb()
db = client["zedox_complete"]

# Collections — names are unchanged so all existing data remains available.
users_col = db["users"]
folders_col = db["folders"]
codes_col = db["codes"]
config_col = db["config"]
custom_buttons_col = db["custom_buttons"]
admins_col = db["admins"]
payments_col = db["payments"]
# New collections only extend the schema; original names remain unchanged.
logs_col = db["logs"]
broadcasts_col = db["broadcasts"]
auto_posts_col = db["auto_posts"]
source_chats_col = db["source_chats"]
point_history_col = db["point_history"]
purchases_col = db["purchases"]
referrals_col = db["referrals"]
backups_col = db["backups"]
promoted_channels_col = db["promoted_channels"]
pending_methods_col = db["pending_methods"]
group_warnings_col = db["group_warnings"]
group_message_log_col = db["group_message_log"]
scam_reports_col = db["scam_reports"]

# Index creation must never prevent the bot from starting.
def ensure_indexes():
    index_jobs = [
        (users_col, "points", {}),
        (users_col, "vip", {}),
        (users_col, "referrals_count", {}),
        (folders_col, [("cat", 1), ("parent", 1)], {}),
        (folders_col, "number", {"unique": True, "sparse": True}),
        (logs_col, [("created_at", -1)], {}),
        (broadcasts_col, [("run_at", 1), ("status", 1)], {}),
        (auto_posts_col, [("next_run", 1), ("active", 1)], {}),
        (point_history_col, [("user_id", 1), ("created_at", -1)], {}),
        (payments_col, [("user_id", 1), ("created_at", -1)], {}),
        (pending_methods_col, [("status", 1), ("created_at", -1)], {}),
        (group_warnings_col, [("group_id", 1), ("user_id", 1)], {"unique": True}),
        (group_message_log_col, [("group_id", 1), ("message_id", 1)], {"unique": True}),
        (scam_reports_col, [("status", 1), ("created_at", -1)], {}),
        (scam_reports_col, [("target_username", 1), ("status", 1)], {}),
    ]
    for collection, keys, options in index_jobs:
        try:
            collection.create_index(keys, **options)
        except Exception as exc:
            print(f"⚠️ Index skipped for {collection.name}: {exc}", flush=True)

ensure_indexes()

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="Markdown", threaded=True, num_threads=int(os.environ.get("BOT_WORKERS", "12")))
# Secondary client with no default parse mode. Use it for user-generated text and legacy posts.
raw_bot = telebot.TeleBot(BOT_TOKEN, parse_mode=None, threaded=False)

# Cache for frequently accessed data
_config_cache = None
_config_cache_time = 0
CACHE_TTL = 30

def get_cached_config():
    global _config_cache, _config_cache_time
    now = time.time()
    if _config_cache and (now - _config_cache_time) < CACHE_TTL:
        return _config_cache
    _config_cache = get_config()
    _config_cache_time = now
    return _config_cache

# =========================
# 🔐 SECURITY
# =========================
def validate_request(message):
    if not message or not message.from_user:
        return False
    if len(message.text or "") > 4096:
        return False
    return True

def hash_user_data(uid):
    secret = os.environ.get("BOT_TOKEN", "secret_key")
    return hmac.new(secret.encode(), str(uid).encode(), hashlib.sha256).hexdigest()[:16]

# =========================
# ⚙️ CONFIG SYSTEM
# =========================
def get_config():
    cfg = config_col.find_one({"_id": "config"})
    if not cfg:
        cfg = {
            "_id": "config",
            "force_channels": [],
            "custom_buttons": [],
            "vip_msg": "💎 Buy VIP to unlock this!",
            "welcome": "🔥 Welcome to ZEDOX BOT",
            "ref_reward": 5,
            "notify": True,
            "purchase_msg": "💰 Purchase VIP to access premium features!",
            "next_folder_number": 1,
            "points_per_dollar": 100,
            "contact_username": None,
            "contact_link": None,
            "vip_contact": None,
            "vip_price": 50,
            "vip_points_price": 5000,
            "payment_methods": ["💳 Binance", "💵 USDT (TRC20)", "💰 Bank Transfer", "🪙 Bitcoin"],
            "referral_vip_count": 50,
            "referral_purchase_count": 10,
            "vip_duration_days": 30,
            "binance_coin": "USDT",
            "binance_network": "TRC20",
            "binance_address": "",
            "binance_memo": "",
            "require_screenshot": True,
            "auto_import_free_source": None,
            "auto_import_vip_source": None,
            "recent_admin_chat_id": None,
            "recent_admin_chat_title": None,
            "hidden_main_buttons": [],
            "force_groups": [],
            "join_notify_group": None,
            "join_notify_enabled": True,
            "method_notify_group": None,
            "method_notify_enabled": True,
            "group_import_notify_enabled": True,
            "manual_methods_list": "",
            "auto_import_free_source": None,
            "auto_import_vip_source": None
        }
        config_col.insert_one(cfg)
    return cfg

def set_config(key, value):
    global _config_cache
    _config_cache = None
    config_col.update_one({"_id": "config"}, {"$set": {key: value}}, upsert=True)


def normalize_chat_reference(value):
    value = _strip_invisible(value or "").strip()
    if not value:
        raise ValueError("Chat/link cannot be empty")
    if re.fullmatch(r"-?\d+", value):
        return int(value)
    m = re.search(r"(?:https?://)?(?:t\.me|telegram\.me)/([A-Za-z0-9_]{5,})", value, re.I)
    if m:
        return "@" + m.group(1)
    if value.startswith("@") and re.fullmatch(r"@[A-Za-z0-9_]{5,}", value):
        return value
    if re.fullmatch(r"[A-Za-z0-9_]{5,}", value):
        return "@" + value
    raise ValueError("Send @username, username, t.me link, or numeric chat ID")

def normalize_url_or_username(value):
    value = _strip_invisible(value or "").strip()
    if value.startswith(("http://", "https://", "tg://")):
        return value
    ref = normalize_chat_reference(value)
    if isinstance(ref, int):
        raise ValueError("A numeric ID cannot be opened as a button link")
    return f"https://t.me/{ref.lstrip('@')}"

def admin_success(uid, text="Process Complete", reply_markup=None):
    # Raw send prevents user supplied names/errors from breaking Markdown parsing.
    raw_bot.send_message(uid, f"✅ {text}", reply_markup=reply_markup or admin_menu())

def admin_error(uid, exc, reply_markup=None):
    raw_bot.send_message(uid, f"❌ Process Failed\n{str(exc)[:1000]}", reply_markup=reply_markup or admin_menu())

def send_method_notification(action, folder):
    cfg = get_cached_config()
    if not cfg.get("method_notify_enabled", True):
        return
    targets = cfg.get("method_notify_groups") or []
    legacy = cfg.get("method_notify_group") or cfg.get("join_notify_group")
    if legacy and legacy not in targets:
        targets.append(legacy)
    if not targets:
        return
    cat = str(folder.get("cat", "")).upper()
    name = str(folder.get("name", "Unknown"))
    price = folder.get("price", 0)
    text = f"🔔 METHOD {str(action).upper()}\n\n📂 Category: {cat}\n📄 Name: {name}\n💰 Price: {price} points"
    for target in targets:
        try:
            raw_bot.send_message(target, text)
        except Exception as exc:
            log_event("method_notification_error", target=target, details={"error": str(exc)}, level="error")

def append_to_manual_methods_list(folder):
    """Append a newly published method to the admin-managed methods list once."""
    try:
        cfg = get_config()
        current = (cfg.get("manual_methods_list") or "").strip()
        name = str(folder.get("name") or "Unnamed Method").strip()
        cat = str(folder.get("cat") or "methods").upper()
        if not name:
            return
        existing_lines = {line.strip().lstrip("•-📌💎🆓📱🛠 ").strip().lower() for line in current.splitlines()}
        if name.lower() in existing_lines:
            return
        addition = f"• {name}"
        current = current + "\n" + addition if current else f"📋 ZEDOX METHODS LIST\n\n{cat}\n{addition}"
        set_config("manual_methods_list", current)
    except Exception as exc:
        log_event("manual_methods_list_append_error", details={"error": str(exc)}, level="error")

# =========================
# 👑 MULTIPLE ADMINS SYSTEM
# =========================
def init_admins():
    if not admins_col.find_one({"_id": ADMIN_ID}):
        admins_col.insert_one({
            "_id": ADMIN_ID,
            "username": None,
            "added_by": "system",
            "added_at": time.time(),
            "is_owner": True
        })

init_admins()

def is_admin(uid):
    uid = int(uid) if isinstance(uid, str) else uid
    if uid == ADMIN_ID:
        return True
    return admins_col.find_one({"_id": uid}) is not None

def add_admin(uid, username=None, added_by=None):
    uid = int(uid) if isinstance(uid, str) else uid
    if admins_col.find_one({"_id": uid}):
        return False
    admins_col.insert_one({
        "_id": uid,
        "username": username,
        "added_by": added_by,
        "added_at": time.time(),
        "is_owner": False
    })
    return True

def remove_admin(uid):
    uid = int(uid) if isinstance(uid, str) else uid
    if uid == ADMIN_ID:
        return False
    result = admins_col.delete_one({"_id": uid})
    return result.deleted_count > 0

def get_all_admins():
    return list(admins_col.find({}))

# =========================
# 👤 USER SYSTEM
# =========================
class User:
    _cache = {}
    _cache_time = {}
    
    def __init__(self, uid):
        self.uid = str(uid)
        
        if uid in self._cache and (time.time() - self._cache_time.get(uid, 0)) < 30:
            self.data = self._cache[uid]
            return
        
        data = users_col.find_one({"_id": self.uid})
        
        if not data:
            data = {
                "_id": self.uid,
                "points": 0,
                "vip": False,
                "vip_expiry": None,
                "ref": None,
                "refs": 0,
                "refs_who_bought_vip": 0,
                "purchased_methods": [],
                "used_codes": [],
                "username": None,
                "created_at": time.time(),
                "last_active": time.time(),
                "hash_id": hash_user_data(uid),
                "total_points_earned": 0,
                "total_points_spent": 0
            }
            users_col.insert_one(data)
        
        self.data = data
        self._cache[uid] = data
        self._cache_time[uid] = time.time()
    
    def save(self):
        users_col.update_one({"_id": self.uid}, {"$set": self.data})
        self._cache[self.uid] = self.data
        self._cache_time[self.uid] = time.time()
    
    def is_vip(self):
        if self.data.get("vip", False):
            expiry = self.data.get("vip_expiry")
            if expiry and expiry < time.time():
                self.data["vip"] = False
                self.data["vip_expiry"] = None
                self.save()
                return False
            return True
        return False
    
    def points(self): 
        return self.data.get("points", 0)
    
    def purchased_methods(self): 
        return self.data.get("purchased_methods", [])
    
    def used_codes(self): 
        return self.data.get("used_codes", [])
    
    def username(self): 
        return self.data.get("username", None)
    
    def update_username(self, username):
        if username != self.data.get("username"):
            self.data["username"] = username
            self.save()
    
    def add_points(self, p):
        self.data["points"] += p
        self.data["total_points_earned"] = self.data.get("total_points_earned", 0) + p
        self.save()
    
    def spend_points(self, p):
        self.data["points"] -= p
        self.data["total_points_spent"] = self.data.get("total_points_spent", 0) + p
        self.save()
    
    def make_vip(self, duration_days=None):
        self.data["vip"] = True
        if duration_days and duration_days > 0:
            self.data["vip_expiry"] = time.time() + (duration_days * 86400)
        else:
            self.data["vip_expiry"] = None
        self.save()
    
    def remove_vip(self):
        self.data["vip"] = False
        self.data["vip_expiry"] = None
        self.save()
    
    def purchase_method(self, method_name, price):
        if self.points() >= price:
            self.spend_points(price)
            if method_name not in self.purchased_methods():
                self.data["purchased_methods"].append(method_name)
                self.save()
            return True
        return False
    
    def can_access_method(self, method_name):
        return self.is_vip() or method_name in self.purchased_methods()
    
    def add_used_code(self, code):
        if code not in self.used_codes():
            self.data["used_codes"].append(code)
            self.save()
            return True
        return False
    
    def has_used_code(self, code):
        return code in self.used_codes()
    
    def add_ref(self):
        self.data["refs"] = self.data.get("refs", 0) + 1
        self.save()
        
        config = get_cached_config()
        required_refs = config.get("referral_vip_count", 50)
        
        if self.data["refs"] >= required_refs and not self.is_vip():
            self.make_vip(config.get("vip_duration_days", 30))
            return True
        return False
    
    def add_ref_bought_vip(self):
        self.data["refs_who_bought_vip"] = self.data.get("refs_who_bought_vip", 0) + 1
        self.save()
        
        config = get_cached_config()
        required_purchases = config.get("referral_purchase_count", 10)
        
        if self.data["refs_who_bought_vip"] >= required_purchases and not self.is_vip():
            self.make_vip(config.get("vip_duration_days", 30))
            return True
        return False
    
    def get_refs_count(self):
        return self.data.get("refs", 0)
    
    def get_refs_bought_vip_count(self):
        return self.data.get("refs_who_bought_vip", 0)

# =========================
# 📁 FOLDER SYSTEM (WITH WORKING SUBFOLDERS)
# =========================
class FS:
    def add(self, cat, name, files, price, parent=None, number=None, text_content=None, at_start=False):
        if number is None:
            config = get_config()
            number = config.get("next_folder_number", 1)
            set_config("next_folder_number", number + 1)
        
        folder_data = {
            "cat": cat,
            "name": name,
            "files": files,
            "price": price,
            "parent": parent,
            "number": number,
            "created_at": time.time(),
            "sort_priority": -time.time() if at_start else 0
        }
        
        if text_content:
            folder_data["text_content"] = text_content
        
        folders_col.insert_one(folder_data)
        return number
    
    def get(self, cat, parent=None):
        query = {"cat": cat}
        if parent:
            query["parent"] = parent
        else:
            query["parent"] = None
        return list(folders_col.find(query).sort([("pinned", -1), ("pinned_at", -1), ("sort_priority", 1), ("created_at", -1)]))
    
    def get_one(self, cat, name, parent=None):
        query = {"cat": cat, "name": name}
        if parent:
            query["parent"] = parent
        return folders_col.find_one(query)
    
    def get_by_number(self, number):
        return folders_col.find_one({"number": number})
    
    def update_numbers_after_delete(self, deleted_number):
        folders_col.update_many(
            {"number": {"$gt": deleted_number}},
            {"$inc": {"number": -1}}
        )
        config = get_config()
        current_next = config.get("next_folder_number", 1)
        if current_next > deleted_number:
            set_config("next_folder_number", current_next - 1)
    
    def delete_all_subfolders(self, cat, parent_name):
        subfolders = list(folders_col.find({"cat": cat, "parent": parent_name}))
        for sub in subfolders:
            self.delete_all_subfolders(cat, sub["name"])
            folders_col.delete_one({"_id": sub["_id"]})
    
    def delete(self, cat, name, parent=None):
        query = {"cat": cat, "name": name}
        if parent:
            query["parent"] = parent
        else:
            query["parent"] = None
        
        folder = folders_col.find_one(query)
        if not folder:
            return False
        
        number = folder.get("number")
        self.delete_all_subfolders(cat, name)
        folders_col.delete_one(query)
        
        if number:
            self.update_numbers_after_delete(number)
        
        return True
    
    def edit_price(self, cat, name, price, parent=None):
        query = {"cat": cat, "name": name}
        if parent:
            query["parent"] = parent
        folders_col.update_one(query, {"$set": {"price": price}})
    
    def edit_name(self, cat, old, new, parent=None):
        query = {"cat": cat, "name": old}
        if parent:
            query["parent"] = parent
        folders_col.update_one(query, {"$set": {"name": new}})
        folders_col.update_many({"cat": cat, "parent": old}, {"$set": {"parent": new}})
    
    def move_folder(self, number, new_parent):
        folders_col.update_one({"number": number}, {"$set": {"parent": new_parent}})
    
    def edit_content(self, cat, name, content_type, content, parent=None):
        query = {"cat": cat, "name": name}
        if parent:
            query["parent"] = parent
        
        if content_type == "text":
            folders_col.update_one(query, {"$set": {"text_content": content}})
        elif content_type == "files":
            folders_col.update_one(query, {"$set": {"files": content}})
        return True

fs = FS()

# =========================
# 🏆 CODES SYSTEM
# =========================
class Codes:
    def generate(self, pts, count, multi_use=False, expiry_days=None):
        res = []
        expiry = time.time() + (expiry_days * 86400) if expiry_days else None
        
        for _ in range(count):
            code = "ZEDOX" + ''.join(random.choices(string.ascii_uppercase+string.digits, k=8))
            while codes_col.find_one({"_id": code}):
                code = "ZEDOX" + ''.join(random.choices(string.ascii_uppercase+string.digits, k=8))
            
            codes_col.insert_one({
                "_id": code,
                "points": pts,
                "used": False,
                "multi_use": multi_use,
                "used_count": 0,
                "max_uses": 0 if not multi_use else 10,
                "expiry": expiry,
                "created_at": time.time(),
                "used_by_users": []
            })
            res.append(code)
        return res
    
    def redeem(self, code, user):
        code_data = codes_col.find_one({"_id": code})
        
        if not code_data:
            return False, 0, "invalid"
        
        if code_data.get("expiry") and time.time() > code_data["expiry"]:
            return False, 0, "expired"
        
        if not code_data.get("multi_use", False) and code_data.get("used", False):
            return False, 0, "already_used"
        
        if user.uid in code_data.get("used_by_users", []):
            return False, 0, "already_used_by_user"
        
        if code_data.get("multi_use", False):
            used_count = code_data.get("used_count", 0)
            max_uses = code_data.get("max_uses", 10)
            if used_count >= max_uses:
                return False, 0, "max_uses_reached"
        
        pts = code_data["points"]
        user.add_points(pts)
        
        update_data = {
            "$push": {"used_by_users": user.uid},
            "$inc": {"used_count": 1}
        }
        
        if not code_data.get("multi_use", False):
            update_data["$set"] = {"used": True}
        
        codes_col.update_one({"_id": code}, update_data)
        user.add_used_code(code)
        
        return True, pts, "success"
    
    def get_all_codes(self):
        return list(codes_col.find({}).sort("created_at", -1))
    
    def get_stats(self):
        total = codes_col.count_documents({})
        used = codes_col.count_documents({"used": True})
        unused = total - used
        multi_use = codes_col.count_documents({"multi_use": True})
        return total, used, unused, multi_use

codesys = Codes()

# =========================
# 📦 POINTS PACKAGES SYSTEM
# =========================
def get_points_packages():
    packages = config_col.find_one({"_id": "points_packages"})
    if not packages:
        default_packages = {
            "_id": "points_packages",
            "packages": [
                {"points": 100, "price": 5, "currency": "USD", "bonus": 0, "active": True},
                {"points": 250, "price": 10, "currency": "USD", "bonus": 25, "active": True},
                {"points": 550, "price": 20, "currency": "USD", "bonus": 100, "active": True},
                {"points": 1500, "price": 50, "currency": "USD", "bonus": 500, "active": True},
                {"points": 3500, "price": 100, "currency": "USD", "bonus": 1500, "active": True},
                {"points": 10000, "price": 250, "currency": "USD", "bonus": 5000, "active": True}
            ]
        }
        config_col.insert_one(default_packages)
        return default_packages["packages"]
    return packages["packages"]

def save_points_packages(packages):
    config_col.update_one(
        {"_id": "points_packages"},
        {"$set": {"packages": packages}},
        upsert=True
    )

# =========================
# 🚫 FORCE JOIN (FAST)
# =========================
_force_cache = {}
FORCE_CACHE_TTL = 10

def force_block(uid):
    global _force_cache
    now = time.time()
    
    if is_admin(uid):
        return False
    
    cfg = get_cached_config()
    force_channels = cfg.get("force_channels", [])
    force_groups = cfg.get("force_groups", [])
    force_targets = list(dict.fromkeys(force_channels + force_groups))
    
    if not force_targets:
        return False
    
    for ch in force_targets:
        try:
            member = bot.get_chat_member(ch, uid)
            if member.status in ["left", "kicked"]:
                kb = InlineKeyboardMarkup()
                for channel in force_targets:
                    kb.add(InlineKeyboardButton(f"📢 Join {channel}", url=f"https://t.me/{channel.replace('@','')}"))
                kb.add(InlineKeyboardButton("✅ I Joined", callback_data="recheck"))
                bot.send_message(uid, "🚫 **Access Restricted!**\n\nPlease join the following channels:", reply_markup=kb, parse_mode="Markdown")
                return True
        except:
            kb = InlineKeyboardMarkup()
            for channel in force_targets:
                kb.add(InlineKeyboardButton(f"📢 Join {channel}", url=f"https://t.me/{channel.replace('@','')}"))
            kb.add(InlineKeyboardButton("✅ I Joined", callback_data="recheck"))
            bot.send_message(uid, f"🚫 **Please join required channels!**", reply_markup=kb, parse_mode="Markdown")
            return True
    
    return False

def force_join_handler(func):
    @wraps(func)
    def wrapper(message):
        if force_block(message.from_user.id):
            return
        return func(message)
    return wrapper

# =========================
# 📱 MAIN MENU
# =========================
def get_custom_buttons():
    cfg = get_cached_config()
    return cfg.get("custom_buttons", [])

def add_custom_button(button_text, button_type, button_data):
    cfg = get_config()
    buttons = cfg.get("custom_buttons", [])
    buttons.append({
        "text": button_text,
        "type": button_type,
        "data": button_data
    })
    set_config("custom_buttons", buttons)

def remove_custom_button(button_text):
    cfg = get_config()
    buttons = cfg.get("custom_buttons", [])
    buttons = [b for b in buttons if b["text"] != button_text]
    set_config("custom_buttons", buttons)

def get_hidden_main_buttons():
    cfg = get_cached_config()
    return set(cfg.get("hidden_main_buttons", []))

MAIN_MENU_ROWS = [
    ("📂 FREE METHODS", "💎 VIP METHODS"),
    ("📦 PREMIUM APPS", "⚡ SERVICES"),
    ("💰 POINTS", "⭐ BUY VIP"),
    ("🎁 REFERRAL", "👤 ACCOUNT"),
    ("📚 MY METHODS", "💎 GET POINTS"),
    ("🆔 CHAT ID", "🏆 REDEEM"),
    ("📋 METHODS LIST",),
    ("📢 CHANNELS", "➕ ADD CHANNEL"),
]

MAIN_MENU_BUTTONS = [button for row in MAIN_MENU_ROWS for button in row]

def main_menu(uid):
    kb = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    hidden = get_hidden_main_buttons()

    for row in MAIN_MENU_ROWS:
        visible = [button for button in row if button not in hidden]
        if visible:
            kb.row(*visible)

    custom_btns = get_custom_buttons()
    if custom_btns:
        row = []
        for btn in custom_btns:
            if btn["text"] in hidden:
                continue
            row.append(btn["text"])
            if len(row) == 2:
                kb.row(*row)
                row = []
        if row:
            kb.row(*row)

    if is_admin(uid):
        kb.row("⚙️ ADMIN PANEL")

    return kb

def finalize_pending_referral(uid, telegram_user=None):
    """Credit a referral only after the referred user passes all force-join checks."""
    uid_str = str(uid)
    row = users_col.find_one({"_id": uid_str}, {"pending_ref": 1, "ref": 1, "username": 1}) or {}
    ref_id = row.get("pending_ref")
    if not ref_id or row.get("ref"):
        return False
    if str(ref_id) == uid_str or not str(ref_id).isdigit():
        users_col.update_one({"_id": uid_str}, {"$unset": {"pending_ref": ""}})
        return False
    if not users_col.find_one({"_id": str(ref_id)}, {"_id": 1}):
        users_col.update_one({"_id": uid_str}, {"$unset": {"pending_ref": ""}})
        return False

    # Claim once. Only the process that changes ref from None/missing gets to reward.
    claimed = users_col.update_one(
        {"_id": uid_str, "$or": [{"ref": None}, {"ref": {"$exists": False}}], "pending_ref": str(ref_id)},
        {"$set": {"ref": str(ref_id)}, "$unset": {"pending_ref": ""}},
    )
    if claimed.modified_count != 1:
        return False

    ref_user = User(str(ref_id))
    reward = int(get_cached_config().get("ref_reward", 5))
    old_balance = ref_user.points()
    ref_user.add_points(reward)
    got_vip = ref_user.add_ref()
    display_name = None
    if telegram_user is not None:
        display_name = getattr(telegram_user, "username", None)
        if not display_name:
            display_name = " ".join(x for x in [getattr(telegram_user, "first_name", None), getattr(telegram_user, "last_name", None)] if x)
    display_name = display_name or row.get("username") or uid_str
    vip_msg = ""
    if got_vip:
        vip_msg = f"\n\n🎊 You reached **{ref_user.get_refs_count()} referrals** and received **FREE VIP ACCESS!**"
    try:
        if not get_cached_config().get("user_referral_notifications_enabled", True):
            return True
        bot.send_message(
            int(ref_id),
            f"🎉 **REFERRAL COMPLETED!** 🎉\n\n"
            f"👤 **{display_name}** joined all required channels and groups.\n"
            f"💫 You received **{reward:,} points!**\n"
            f"💰 Previous balance: **{old_balance:,}**\n"
            f"🏆 New balance: **{ref_user.points():,}**\n"
            f"👥 Total referrals: **{ref_user.get_refs_count()}**{vip_msg}\n\n"
            f"🥳 Enjoy your reward! 🚀",
            parse_mode="Markdown",
        )
    except Exception as exc:
        log_event("referral_reward_notify_error", ref_id, uid, {"error": str(exc)}, level="error")
    return True

# =========================
# 🚀 START
# =========================
@bot.message_handler(commands=["start"])
def start_cmd(m):
    if not validate_request(m):
        return
    
    uid = m.from_user.id
    args = m.text.split()
    is_new_user = users_col.find_one({"_id": str(uid)}, {"_id": 1}) is None
    
    user = User(uid)
    
    if m.from_user.username:
        user.update_username(m.from_user.username)
    users_col.update_one(
        {"_id": str(uid)},
        {"$set": {
            "first_name": m.from_user.first_name or "",
            "last_name": m.from_user.last_name or "",
            "username": m.from_user.username or user.data.get("username"),
            "last_active": time.time(),
        }},
    )
    
    if len(args) > 1:
        ref_id = args[1].strip()
        if ref_id != str(uid) and ref_id.isdigit() and not user.data.get("ref"):
            # Do not credit now. It becomes valid only after all force-join requirements pass.
            if users_col.find_one({"_id": ref_id}, {"_id": 1}):
                users_col.update_one(
                    {"_id": str(uid), "$or": [{"ref": None}, {"ref": {"$exists": False}}]},
                    {"$set": {"pending_ref": ref_id}},
                )
                user.data["pending_ref"] = ref_id

    if force_block(uid):
        return

    finalize_pending_referral(uid, m.from_user)
    user = User(uid)
    cfg = get_cached_config()
    welcome_msg = cfg.get("welcome", "Welcome to ZEDOX BOT!")
    
    first_name = getattr(m.from_user, "first_name", None) or "Member"
    welcome_text = (
        f"✨ WELCOME, {first_name.upper()}! ✨\n\n"
        f"{welcome_msg}\n\n"
        "╭━━━━━━━━━━━━━━━━━━╮\n"
        f"💰 Points Balance: {user.points()}\n"
        f"💎 VIP Status: {'ACTIVE' if user.is_vip() else 'NOT ACTIVE'}\n"
        "╰━━━━━━━━━━━━━━━━━━╯\n\n"
        "📂 Explore FREE and VIP methods\n"
        "🎁 Invite friends to earn points\n"
        "🏆 Redeem codes and unlock rewards\n\n"
        "Choose an option from the menu below 👇"
    )
    raw_bot.send_message(uid, welcome_text, reply_markup=main_menu(uid))

    if is_new_user:
        guide_text = (
            "📘 QUICK START GUIDE\n\n"
            "📂 FREE METHODS — Browse free methods and resources.\n"
            "💎 VIP METHODS — View premium methods; buy with points or unlock with VIP.\n"
            "💰 POINTS — Check your balance, earnings, spending and rewards.\n"
            "⭐ BUY VIP — See VIP price, benefits and payment details.\n"
            "🎁 REFERRAL — Share your personal link and earn points after your friend joins every required group/channel.\n"
            "📚 MY METHODS — Open methods you already purchased.\n"
            "💎 GET POINTS — View point packages and contact the admin.\n"
            "🏆 REDEEM — Redeem a valid points code.\n"
            "📋 METHODS LIST — View the available method names.\n\n"
            "🛡 SAFETY COMMANDS\n"
            "• /scammer @username — Submit a scam report.\n"
            "• Reply with /scammer — Report the person you replied to.\n"
            "• /check @username — Check scam-report status.\n"
            "• /scammerlist — View reported accounts.\n\n"
            "Use the menu buttons below whenever you need a feature. 🚀"
        )
        raw_bot.send_message(uid, guide_text, reply_markup=main_menu(uid), disable_web_page_preview=True)

        notify_group = cfg.get("join_notify_group")
        if notify_group and cfg.get("join_notify_enabled", True):
            try:
                full_name = " ".join(x for x in [m.from_user.first_name, m.from_user.last_name] if x) or "Unknown"
                username = f"@{m.from_user.username}" if m.from_user.username else "No username"
                referrer = user.data.get("ref") or "Direct join"
                bot.send_message(
                    notify_group,
                    f"🆕 **New User Joined Bot**\n\n"
                    f"👤 Name: {full_name}\n"
                    f"🔗 Username: {username}\n"
                    f"🆔 User ID: `{uid}`\n"
                    f"🌐 Language: {m.from_user.language_code or 'Unknown'}\n"
                    f"🎁 Referrer: `{referrer}`\n"
                    f"🕒 Joined: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                    parse_mode="Markdown",
                )
            except Exception as exc:
                log_event("join_notification_error", uid, notify_group, {"error": str(exc)}, level="error")

# =========================
# 💰 POINTS COMMAND
# =========================
@bot.message_handler(func=lambda m: m.text == "💰 POINTS")
@force_join_handler
def points_cmd(m):
    uid = m.from_user.id
    user = User(uid)
    cfg = get_cached_config()
    points = int(user.points() or 0)
    purchased_count = len(user.purchased_methods())
    ref_count = user.get_refs_count()
    ref_bought_count = user.get_refs_bought_vip_count()
    vip_text = "ACTIVE 👑" if user.is_vip() else "Not active"

    text = (
        "💎  ZEDOX POINTS WALLET  💎\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"💰 Current balance: {points:,} points\n"
        f"👑 VIP status: {vip_text}\n"
        f"📚 Purchased methods: {purchased_count}\n\n"
        "📊  YOUR ACTIVITY\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"🎁 Verified referrals: {ref_count}\n"
        f"⭐ Referral purchases: {ref_bought_count}\n"
        f"✨ Total points earned: {int(user.data.get('total_points_earned', 0)):,}\n"
        f"🛍 Total points spent: {int(user.data.get('total_points_spent', 0)):,}\n\n"
        "🚀  EARN MORE POINTS\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "• Invite friends using your referral link\n"
        "• Redeem reward codes\n"
        "• Purchase a points package\n\n"
        f"🎯 {cfg.get('referral_vip_count', 50)} verified referrals = FREE VIP\n"
        f"🎯 {cfg.get('referral_purchase_count', 10)} referral purchases = FREE VIP"
    )
    kb = InlineKeyboardMarkup(row_width=2)
    kb.row(
        InlineKeyboardButton("💎 Buy Points", callback_data="open_points_shop"),
        InlineKeyboardButton("🎁 Referral", callback_data="open_referral_card"),
    )
    kb.row(InlineKeyboardButton("🔄 Refresh Balance", callback_data="check_balance"))
    raw_bot.send_message(uid, text, reply_markup=kb)

@bot.callback_query_handler(func=lambda c: c.data == "open_points_shop")
def open_points_shop_callback(c):
    bot.answer_callback_query(c.id)
    send_points_shop(c.from_user.id)

# =========================
# 💎 GET POINTS
# =========================
def send_points_shop(uid):
    user = User(uid)
    packages = get_points_packages()
    active_packages = [p for p in packages if p.get("active", True)]
    cfg = get_cached_config()
    contact_username = cfg.get("contact_username")
    contact_link = cfg.get("contact_link")

    message = "💎 **BUY POINTS** 💎\n\n"
    message += f"💰 Your balance: **{user.points():,} points**\n\n"
    if active_packages:
        message += "📦 **AVAILABLE PACKAGES**\n\n"
        for pkg in active_packages:
            base = int(pkg.get("points", 0))
            bonus = int(pkg.get("bonus", 0))
            total = base + bonus
            price = pkg.get("price", 0)
            message += f"💠 **{total:,} points** — **${price}**"
            if bonus:
                message += f"  _(includes +{bonus:,} bonus)_"
            message += "\n"
    else:
        message += "❌ No point packages are currently available.\n"
    message += "\n📩 Tap **Contact Admin** to purchase a package."

    kb = InlineKeyboardMarkup(row_width=1)
    contact_url = None
    if contact_link:
        contact_url = contact_link.strip()
    elif contact_username:
        contact_url = f"https://t.me/{contact_username.replace('@', '').strip()}"
    if contact_url:
        kb.add(InlineKeyboardButton("📞 Contact Admin", url=contact_url))
    kb.add(InlineKeyboardButton("🔄 Refresh Balance", callback_data="check_balance"))
    bot.send_message(uid, message, reply_markup=kb, parse_mode="Markdown")

@bot.message_handler(func=lambda m: m.text == "💎 GET POINTS")
@force_join_handler
def get_points_button(m):
    send_points_shop(m.from_user.id)

# =========================
# 📂 SHOW FOLDERS (FAST)
# =========================
def get_folders_kb(cat, parent=None, page=0, items_per_page=15):
    data = fs.get(cat, parent)
    
    start = page * items_per_page
    end = start + items_per_page
    page_items = data[start:end]
    
    kb = InlineKeyboardMarkup(row_width=2)
    
    for item in page_items:
        name = item["name"]
        price = item.get("price", 0)
        # Check if has subfolders
        subfolders = fs.get(cat, name)
        icon = "📁" if subfolders else "📄"
        pin = "📌 " if item.get("pinned") else ""
        expired = "⛔ EXPIRED • " if item.get("expired") else ""
        text = f"{pin}{expired}{icon} {name}"
        if price > 0:
            text += f" ({price} pts)"
        
        kb.add(InlineKeyboardButton(text, callback_data=f"openid|{item['_id']}"))
    
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("⬅️", callback_data=f"page|{cat}|{page-1}|{parent or ''}"))
    if end < len(data):
        nav_buttons.append(InlineKeyboardButton("➡️", callback_data=f"page|{cat}|{page+1}|{parent or ''}"))
    
    if nav_buttons:
        kb.row(*nav_buttons)
    
    if parent:
        kb.add(InlineKeyboardButton("🔙 Back", callback_data=f"back|{cat}|{parent}"))
    
    return kb

@bot.message_handler(func=lambda m: m.text in [
    "📂 FREE METHODS",
    "💎 VIP METHODS",
    "📦 PREMIUM APPS",
    "⚡ SERVICES"
])
@force_join_handler
def show_category(m):
    uid = m.from_user.id
    
    mapping = {
        "📂 FREE METHODS": "free",
        "💎 VIP METHODS": "vip",
        "📦 PREMIUM APPS": "apps",
        "⚡ SERVICES": "services"
    }
    
    cat = mapping.get(m.text)
    
    if cat is None:
        bot.send_message(uid, "❌ Invalid category")
        return
    
    try:
        data = fs.get(cat)
        if not data:
            raw_bot.send_message(uid, f"📂 {m.text}\n\nNo methods available yet.", reply_markup=main_menu(uid))
            return
        raw_bot.send_message(uid, f"📂 {m.text}\n\nChoose a method:", reply_markup=get_folders_kb(cat))
    except Exception as exc:
        log_event("show_category_error", uid, details={"category": cat, "error": str(exc)}, level="error")
        raw_bot.send_message(uid, f"❌ Could not open {m.text}.\nPlease try again.\n\nError: {str(exc)[:300]}", reply_markup=main_menu(uid))

# =========================
# 📂 OPEN FOLDER (WITH WORKING SUBFOLDERS)
# =========================
@bot.callback_query_handler(func=lambda c: c.data.startswith("open|"))
def open_folder(c):
    uid = c.from_user.id
    user = User(uid)
    
    parts = c.data.split("|")
    cat = parts[1]
    name = parts[2]
    parent = parts[3] if len(parts) > 3 and parts[3] else None
    
    folder = fs.get_one(cat, name, parent if parent else None)
    
    if not folder:
        bot.answer_callback_query(c.id, "❌ Folder not found")
        return

    if folder.get("expired"):
        bot.answer_callback_query(c.id, "⛔ This method has expired", True)
        raw_bot.send_message(uid, f"⛔ METHOD EXPIRED\n\n{folder.get('name', 'This method')} is currently unavailable.\nPlease wait for an updated version.")
        return
    
    # CHECK FOR SUBFOLDERS - THIS IS THE KEY
    subfolders = fs.get(cat, name)
    
    if subfolders and len(subfolders) > 0:
        # Show subfolders
        kb = InlineKeyboardMarkup(row_width=1)
        
        for sub in subfolders:
            sub_name = sub["name"]
            sub_number = sub.get("number", "?")
            sub_price = sub.get("price", 0)
            
            # Check deeper subfolders
            deeper = fs.get(cat, sub_name)
            icon = "📁" if deeper else "📄"
            
            text = f"{icon} {sub_name}"
            if sub_price > 0:
                text += f" ({sub_price} pts)"
            
            kb.add(InlineKeyboardButton(text, callback_data=f"openid|{sub['_id']}"))
        
        kb.add(InlineKeyboardButton("🔙 BACK", callback_data=f"back|{cat}|{name}"))
        
        bot.edit_message_text(
            f"📁 <b>{name}</b>",
            uid,
            c.message.message_id,
            reply_markup=kb,
            parse_mode="HTML"
        )
        bot.answer_callback_query(c.id)
        return
    
    # Handle text content
    text_content = folder.get("text_content")
    if text_content and not folder.get("files"):
        price = folder.get("price", 0)
        
        if cat == "vip":
            if user.is_vip() or user.can_access_method(name):
                pass
            else:
                if price > 0:
                    buy_kb = InlineKeyboardMarkup(row_width=2)
                    buy_kb.add(
                        InlineKeyboardButton(f"💰 Buy {price} pts", callback_data=f"buyid|{folder['_id']}|{price}"),
                        InlineKeyboardButton("⭐ VIP", callback_data="get_vip"),
                        InlineKeyboardButton("💎 Points", callback_data="get_points")
                    )
                    buy_kb.add(InlineKeyboardButton("❌ Cancel", callback_data="cancel_buy"))
                    bot.answer_callback_query(c.id, "🔒 VIP method")
                    bot.send_message(uid, f"🔒 **{name}**\n\nPrice: {price} pts\nYour points: {user.points()}", reply_markup=buy_kb, parse_mode="Markdown")
                else:
                    buy_kb = InlineKeyboardMarkup(row_width=2)
                    buy_kb.add(
                        InlineKeyboardButton("⭐ VIP", callback_data="get_vip"),
                        InlineKeyboardButton("💎 Points", callback_data="get_points")
                    )
                    bot.answer_callback_query(c.id, "🔒 VIP only")
                    bot.send_message(uid, f"🔒 **{name}**\nVIP only!", reply_markup=buy_kb, parse_mode="Markdown")
                return
        
        if cat != "vip" and price > 0 and not user.is_vip():
            if user.points() < price:
                bot.answer_callback_query(c.id, f"❌ Need {price} pts! You have {user.points()}", True)
                return
            user.spend_points(price)
            bot.answer_callback_query(c.id, f"✅ -{price} pts")
        
        bot.send_message(uid, f"📄 **{name}**\n\n{text_content}", parse_mode="Markdown")
        
        if cat == "vip" and not user.is_vip():
            user.purchase_method(name, 0)
        
        return
    
    # Handle files
    files = folder.get("files", [])
    price = folder.get("price", 0)
    
    if cat == "vip":
        if user.is_vip() or user.can_access_method(name):
            pass
        else:
            if price > 0:
                buy_kb = InlineKeyboardMarkup(row_width=2)
                buy_kb.add(
                    InlineKeyboardButton(f"💰 Buy {price} pts", callback_data=f"buyid|{folder['_id']}|{price}"),
                    InlineKeyboardButton("⭐ VIP", callback_data="get_vip"),
                    InlineKeyboardButton("💎 Points", callback_data="get_points")
                )
                buy_kb.add(InlineKeyboardButton("❌ Cancel", callback_data="cancel_buy"))
                bot.answer_callback_query(c.id, "🔒 VIP method")
                bot.send_message(uid, f"🔒 **{name}**\n\nPrice: {price} pts\nYour points: {user.points()}", reply_markup=buy_kb, parse_mode="Markdown")
            else:
                buy_kb = InlineKeyboardMarkup(row_width=2)
                buy_kb.add(
                    InlineKeyboardButton("⭐ VIP", callback_data="get_vip"),
                    InlineKeyboardButton("💎 Points", callback_data="get_points")
                )
                bot.answer_callback_query(c.id, "🔒 VIP only")
                bot.send_message(uid, f"🔒 **{name}**\nVIP only!", reply_markup=buy_kb, parse_mode="Markdown")
            return
    
    if cat != "vip" and price > 0 and not user.is_vip():
        if user.points() < price:
            bot.answer_callback_query(c.id, f"❌ Need {price} pts! You have {user.points()}", True)
            return
        user.spend_points(price)
        bot.answer_callback_query(c.id, f"✅ -{price} pts")
    
    if files:
        bot.answer_callback_query(c.id, "📤 Sending...")
        count = 0
        for f in files:
            try:
                bot.copy_message(uid, f["chat"], f["msg"])
                count += 1
                time.sleep(0.1)
            except:
                continue
        
        if get_cached_config().get("notify", True):
            if count > 0:
                bot.send_message(uid, f"✅ {count} file(s) sent!")
            else:
                bot.send_message(uid, "❌ Failed to send.")
    else:
        bot.send_message(uid, "📁 No files.")
    
    if cat == "vip" and not user.is_vip():
        user.purchase_method(name, 0)

# =========================
# 🔙 BACK BUTTON
# =========================
@bot.callback_query_handler(func=lambda c: c.data.startswith("back|"))
def back_handler(c):
    _, cat, current_parent = c.data.split("|")
    
    parent_folder = fs.get_one(cat, current_parent)
    if parent_folder:
        grand_parent = parent_folder.get("parent")
        bot.edit_message_reply_markup(
            c.from_user.id,
            c.message.message_id,
            reply_markup=get_folders_kb(cat, grand_parent)
        )
    else:
        bot.edit_message_reply_markup(
            c.from_user.id,
            c.message.message_id,
            reply_markup=get_folders_kb(cat)
        )
    bot.answer_callback_query(c.id)

# =========================
# 📄 PAGINATION
# =========================
@bot.callback_query_handler(func=lambda c: c.data.startswith("page|"))
def page_handler(c):
    _, cat, page, parent = c.data.split("|")
    parent = parent if parent != "None" else None
    
    try:
        bot.edit_message_reply_markup(
            c.from_user.id,
            c.message.message_id,
            reply_markup=get_folders_kb(cat, parent, int(page))
        )
    except:
        pass
    bot.answer_callback_query(c.id)

# =========================
# 💰 BUY METHOD
# =========================
@bot.callback_query_handler(func=lambda c: c.data.startswith("buy|"))
def buy_method(c):
    uid = c.from_user.id
    user = User(uid)
    
    try:
        _, cat, method_name, price = c.data.split("|")
        price = int(price)
    except:
        bot.answer_callback_query(c.id, "Invalid")
        return

    folder = folders_col.find_one({"cat": cat, "name": method_name})
    if folder and folder.get("expired"):
        bot.answer_callback_query(c.id, "⛔ This method has expired", True)
        raw_bot.send_message(uid, f"⛔ METHOD EXPIRED\n\n{method_name} is unavailable and cannot be purchased or opened.")
        return
    
    if user.is_vip():
        bot.answer_callback_query(c.id, "✅ You are VIP!", True)
        open_folder(c)
        return
    
    if user.can_access_method(method_name):
        bot.answer_callback_query(c.id, "✅ You own this!", True)
        open_folder(c)
        return
    
    if user.points() < price:
        bot.answer_callback_query(c.id, f"❌ Need {price} pts! You have {user.points()}", True)
        return
    
    if user.purchase_method(method_name, price):
        bot.answer_callback_query(c.id, f"✅ Purchased! -{price} pts", True)
        bot.edit_message_text(
            f"✅ **Purchased!**\n\nYou now own: {method_name}\nRemaining: {user.points()} pts",
            uid,
            c.message.message_id,
            parse_mode="Markdown"
        )
    else:
        bot.answer_callback_query(c.id, "❌ Failed!", True)

# =========================
# CALLBACK HANDLERS
# =========================
@bot.callback_query_handler(func=lambda c: c.data == "get_vip")
def get_vip_callback(c):
    uid = c.from_user.id
    user = User(uid)
    cfg = get_cached_config()
    
    if user.is_vip():
        bot.answer_callback_query(c.id, "✅ Already VIP!", True)
        return
    
    vip_msg = cfg.get("vip_msg", "💎 Buy VIP!")
    vip_price_usd = cfg.get("vip_price", 50)
    vip_price_points = cfg.get("vip_points_price", 5000)
    vip_contact = cfg.get("vip_contact")
    
    binance_address = cfg.get("binance_address", "")
    binance_coin = cfg.get("binance_coin", "USDT")
    binance_network = cfg.get("binance_network", "TRC20")
    binance_memo = cfg.get("binance_memo", "")
    
    message = f"💎 **VIP**\n\n{vip_msg}\n\n💰 Price:\n• ${vip_price_usd} USD\n• {vip_price_points} points\n\n"
    
    if binance_address:
        message += f"💳 **Binance:**\nCoin: {binance_coin}\nNetwork: {binance_network}\nAddress: `{binance_address}`\n"
        if binance_memo:
            message += f"Memo: `{binance_memo}`\n"
        message += f"Amount: ${vip_price_usd}\n\n"
    
    message += f"✨ Benefits:\n• All VIP methods\n• Priority support\n• No points needed\n\n"
    
    if vip_contact:
        message += f"📞 Contact: {vip_contact}\n"
    
    message += f"\n🆔 ID: `{uid}`\n💰 Points: {user.points()}"
    
    kb = InlineKeyboardMarkup()
    if user.points() >= vip_price_points:
        kb.add(InlineKeyboardButton(f"⭐ Buy with {vip_price_points} pts", callback_data="buy_vip_points"))
    if vip_contact:
        if vip_contact.startswith("http"):
            kb.add(InlineKeyboardButton("📞 Contact", url=vip_contact))
        elif vip_contact.startswith("@"):
            kb.add(InlineKeyboardButton("📞 Contact", url=f"https://t.me/{vip_contact.replace('@', '')}"))
    
    bot.edit_message_text(message, uid, c.message.message_id, reply_markup=kb if kb.keyboard else None, parse_mode="Markdown")
    bot.answer_callback_query(c.id)

@bot.callback_query_handler(func=lambda c: c.data == "buy_vip_points")
def buy_vip_points_callback(c):
    uid = c.from_user.id
    user = User(uid)
    cfg = get_cached_config()
    vip_price_points = cfg.get("vip_points_price", 5000)
    
    if user.is_vip():
        bot.answer_callback_query(c.id, "✅ Already VIP!", True)
        return
    
    if user.points() >= vip_price_points:
        user.spend_points(vip_price_points)
        user.make_vip(cfg.get("vip_duration_days", 30))
        bot.answer_callback_query(c.id, f"✅ VIP Purchased! -{vip_price_points} pts", True)
        bot.edit_message_text(
            f"🎉 **CONGRATULATIONS!** 🎉\n\nYou are now VIP!\n\n💰 Points: {user.points()}",
            uid,
            c.message.message_id,
            parse_mode="Markdown"
        )
    else:
        bot.answer_callback_query(c.id, f"❌ Need {vip_price_points} pts! You have {user.points()}", True)

@bot.callback_query_handler(func=lambda c: c.data == "get_points")
def get_points_callback(c):
    if force_block(c.from_user.id):
        bot.answer_callback_query(c.id, "Join required chats first", True)
        return
    send_points_shop(c.from_user.id)
    bot.answer_callback_query(c.id)

@bot.callback_query_handler(func=lambda c: c.data == "cancel_buy")
def cancel_buy(c):
    bot.edit_message_text("❌ Cancelled", c.from_user.id, c.message.message_id)
    bot.answer_callback_query(c.id)

@bot.callback_query_handler(func=lambda c: c.data == "check_balance")
def check_balance_callback(c):
    uid = c.from_user.id
    user = User(uid)
    
    bot.answer_callback_query(c.id, f"💰 Balance: {user.points()} pts", True)
    bot.edit_message_text(
        f"💰 **Balance**\n\nPoints: {user.points()}\nVIP: {'✅' if user.is_vip() else '❌'}\nReferrals: {user.get_refs_count()}",
        uid,
        c.message.message_id,
        parse_mode="Markdown"
    )

@bot.callback_query_handler(func=lambda c: c.data == "get_referral")
def get_referral_callback(c):
    uid = c.from_user.id
    cfg = get_cached_config()
    link = f"https://t.me/{bot.get_me().username}?start={uid}"
    
    bot.edit_message_text(
        f"🎁 **Referral Link**\n\n`{link}`\n\n✨ Rewards:\n• +{cfg.get('ref_reward', 5)} pts per referral\n• {cfg.get('referral_vip_count', 50)} referrals → FREE VIP\n• {cfg.get('referral_purchase_count', 10)} referral purchases → FREE VIP",
        uid,
        c.message.message_id,
        parse_mode="Markdown"
    )

@bot.callback_query_handler(func=lambda c: c.data == "get_vip_info")
def get_vip_info_callback(c):
    uid = c.from_user.id
    cfg = get_cached_config()
    vip_contact = cfg.get("vip_contact")
    vip_price_usd = cfg.get("vip_price", 50)
    vip_price_points = cfg.get("vip_points_price", 5000)
    
    message = f"⭐ **VIP Benefits** ⭐\n\n✨ Why become VIP?\n• ALL VIP methods\n• No points needed\n• Priority support\n• Exclusive content\n\n💰 Price: ${vip_price_usd} or {vip_price_points} pts\n\n🎁 FREE VIP:\n• Invite {cfg.get('referral_vip_count', 50)} users\n• Get {cfg.get('referral_purchase_count', 10)} referrals to buy VIP\n\n"
    
    if vip_contact:
        message += f"📞 Contact: {vip_contact}"
    
    kb = InlineKeyboardMarkup()
    if vip_contact:
        if vip_contact.startswith("http"):
            kb.add(InlineKeyboardButton("📞 Contact", url=vip_contact))
        elif vip_contact.startswith("@"):
            kb.add(InlineKeyboardButton("📞 Contact", url=f"https://t.me/{vip_contact.replace('@', '')}"))
    
    bot.edit_message_text(message, uid, c.message.message_id, reply_markup=kb if kb.keyboard else None, parse_mode="Markdown")

@bot.callback_query_handler(func=lambda c: c.data == "recheck")
def recheck(c):
    uid = c.from_user.id
    user = User(uid)
    
    if not force_block(uid):
        try:
            bot.edit_message_text("✅ **Access Granted!**", uid, c.message.message_id, parse_mode="Markdown")
        except:
            pass
        bot.send_message(uid, f"🎉 Welcome!\n\n💰 Points: {user.points()}", reply_markup=main_menu(uid))
    else:
        bot.answer_callback_query(c.id, "❌ Join channels first!", True)

# =========================
# 📚 MY METHODS
# =========================
@bot.message_handler(func=lambda m: m.text == "📚 MY METHODS")
@force_join_handler
def show_purchased_methods(m):
    uid = m.from_user.id
    user = User(uid)
    
    purchased = user.purchased_methods()
    
    if user.is_vip():
        bot.send_message(uid, "💎 **VIP Member**\n\nAccess to ALL VIP methods!", parse_mode="Markdown")
        return
    
    if not purchased:
        bot.send_message(uid, f"📚 **Your Methods**\n\nNo purchased methods yet.\n\n💰 Points: {user.points()}", parse_mode="Markdown")
        return
    
    all_vip_methods = {item["name"]: item for item in fs.get("vip")}
    
    kb = InlineKeyboardMarkup(row_width=2)
    for method in purchased:
        row = all_vip_methods.get(method)
        if row:
            kb.add(InlineKeyboardButton(f"📄 {method}", callback_data=f"openid|{row['_id']}"))
    
    bot.send_message(uid, f"📚 **Your Methods** ({len(purchased)})\n\n💰 Points: {user.points()}", reply_markup=kb, parse_mode="Markdown")


# =========================
# 📋 BEAUTIFUL / ADMIN-EDITABLE METHODS LIST
# =========================
@bot.message_handler(func=lambda m: m.text == "📋 METHODS LIST")
@force_join_handler
def methods_list_cmd(m):
    cfg = get_cached_config()
    manual = (cfg.get("manual_methods_list") or "").strip()
    if manual:
        remaining = manual
        while remaining:
            if len(remaining) <= 4096:
                raw_bot.send_message(m.from_user.id, remaining)
                break
            cut = remaining.rfind("\n", 0, 4096)
            if cut < 100:
                cut = 4096
            raw_bot.send_message(m.from_user.id, remaining[:cut])
            remaining = remaining[cut:].lstrip("\n")
        return

    categories = [
        ("free", "🆓 FREE METHODS", "▫️"),
        ("vip", "👑 VIP METHODS", "▫️"),
        ("apps", "📱 PREMIUM APPS", "▫️"),
        ("services", "🛠 SERVICES", "▫️"),
    ]
    sections, total = [], 0
    for cat, title, icon in categories:
        rows = fs.get(cat)
        if not rows:
            continue
        total += len(rows)
        lines = [f"{title}  •  {len(rows)}"]
        for row in rows:
            pin = "📌 " if row.get("pinned") else ""
            expired = "⛔ " if row.get("expired") else ""
            lines.append(f"{pin}{expired}{icon} {row.get('name', 'Unnamed Method')}")
        sections.append("\n".join(lines))
    text = (
        "╔════════════════════╗\n"
        "      📋 ZEDOX METHODS\n"
        "╚════════════════════╝\n\n"
        f"✨ Available methods: {total}\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        + ("\n\n━━━━━━━━━━━━━━━━━━━━\n\n".join(sections) if sections else "No methods are available yet.")
        + "\n\n━━━━━━━━━━━━━━━━━━━━\n🚀 Select a category from the main menu to open a method."
    )
    while text:
        if len(text) <= 4096:
            raw_bot.send_message(m.from_user.id, text)
            break
        cut = text.rfind("\n", 0, 4096)
        if cut < 100:
            cut = 4096
        raw_bot.send_message(m.from_user.id, text[:cut])
        text = text[cut:].lstrip("\n")

@bot.message_handler(func=lambda m: m.text == "📝 Edit Methods List" and is_admin(m.from_user.id))
def edit_methods_list_menu(m):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("✍️ Send New Manual List", callback_data="methodslist|edit"))
    kb.add(InlineKeyboardButton("♻️ Rebuild Automatically", callback_data="methodslist|rebuild"))
    kb.add(InlineKeyboardButton("🗑 Clear Manual List", callback_data="methodslist|clear"))
    current = (get_config().get("manual_methods_list") or "").strip()
    status = "Manual list is active." if current else "Automatic list is active."
    raw_bot.send_message(m.from_user.id, f"📋 METHODS LIST MANAGER\n\n{status}", reply_markup=kb)

@bot.callback_query_handler(func=lambda c: c.data.startswith("methodslist|"))
def methods_list_admin_cb(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admins only", True)
    action = c.data.split("|", 1)[1]
    try:
        if action == "edit":
            msg = raw_bot.send_message(c.from_user.id, "Send the complete methods list exactly as users should see it.\n\nYou can use plain text, emojis and multiple lines.")
            bot.register_next_step_handler(msg, save_manual_methods_list)
            return bot.answer_callback_query(c.id, "Send the list now")
        if action == "clear":
            set_config("manual_methods_list", "")
            admin_success(c.from_user.id, "Manual methods list cleared. Automatic list is active.")
            return bot.answer_callback_query(c.id, "Cleared")
        if action == "rebuild":
            lines = ["📋 ZEDOX METHODS LIST", ""]
            for cat, title in (("free","🆓 FREE METHODS"),("vip","👑 VIP METHODS"),("apps","📱 PREMIUM APPS"),("services","🛠 SERVICES")):
                rows = fs.get(cat)
                if not rows:
                    continue
                lines.append(title)
                for row in rows:
                    prefix = "📌 " if row.get("pinned") else "• "
                    suffix = " ⛔ EXPIRED" if row.get("expired") else ""
                    lines.append(f"{prefix}{row.get('name','Unnamed Method')}{suffix}")
                lines.append("")
            set_config("manual_methods_list", "\n".join(lines).strip())
            admin_success(c.from_user.id, "Methods list rebuilt from current bot methods.")
            return bot.answer_callback_query(c.id, "Rebuilt")
    except Exception as exc:
        admin_error(c.from_user.id, exc)

def save_manual_methods_list(m):
    try:
        text = (m.text or m.caption or "").strip()
        if not text:
            raise ValueError("The methods list cannot be empty")
        if len(text) > 50000:
            raise ValueError("The methods list is too long")
        set_config("manual_methods_list", text)
        admin_success(m.from_user.id, "Manual methods list saved successfully.")
    except Exception as exc:
        admin_error(m.from_user.id, exc)

# =========================
# 👤 ACCOUNT
# =========================
@bot.message_handler(func=lambda m: m.text == "👤 ACCOUNT")
@force_join_handler
def account_cmd(m):
    uid = m.from_user.id
    user = User(uid)
    
    status = "💎 VIP" if user.is_vip() else "🆓 Free"
    purchased_count = len(user.purchased_methods())
    ref_count = user.get_refs_count()
    ref_bought_count = user.get_refs_bought_vip_count()
    
    account_text = f"**👤 Account**\n\n"
    account_text += f"┌ Status: {status}\n"
    account_text += f"├ Points: {user.points()}\n"
    account_text += f"├ Referrals: {ref_count}\n"
    account_text += f"├ Referral Purchases: {ref_bought_count}\n"
    account_text += f"├ Purchased: {purchased_count} methods\n"
    account_text += f"├ Earned: {user.data.get('total_points_earned', 0)}\n"
    account_text += f"└ Spent: {user.data.get('total_points_spent', 0)}\n\n"
    
    if not user.is_vip():
        cfg = get_cached_config()
        account_text += f"💡 **FREE VIP:**\n• Invite {cfg.get('referral_vip_count', 50)} users\n• Get {cfg.get('referral_purchase_count', 10)} referrals to buy VIP\n"
    
    account_text += f"\n🆔 ID: `{uid}`"
    
    bot.send_message(uid, account_text, parse_mode="Markdown")

# =========================
# 🎁 REFERRAL
# =========================
def send_referral_card(uid):
    user = User(uid)
    cfg = get_cached_config()
    bot_username = bot.get_me().username
    link = f"https://t.me/{bot_username}?start={uid}"
    refs = int(user.get_refs_count() or 0)
    reward = int(cfg.get("ref_reward", 5) or 0)
    target = int(cfg.get("referral_vip_count", 50) or 50)
    bought = int(user.get_refs_bought_vip_count() or 0)
    bought_target = int(cfg.get("referral_purchase_count", 10) or 10)
    progress = min(100, int((refs / target) * 100)) if target else 100
    filled = min(10, progress // 10)
    bar = "🟩" * filled + "⬜" * (10 - filled)

    text = (
        "🎁  INVITE & EARN  🎁\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "Share your personal link and earn rewards after your friend joins all required channels and groups.\n\n"
        f"🔗 Your referral link:\n{link}\n\n"
        "📊  YOUR PROGRESS\n"
        f"{bar}  {progress}%\n"
        f"👥 Verified referrals: {refs}/{target}\n"
        f"💫 Referral points earned: {refs * reward:,}\n"
        f"👑 Referral purchases: {bought}/{bought_target}\n\n"
        "🏆  REWARDS\n"
        f"• +{reward} points for every verified referral\n"
        f"• {target} verified referrals unlock FREE VIP\n"
        f"• {bought_target} referral purchases unlock FREE VIP\n\n"
        f"💰 Current balance: {int(user.points()):,} points"
    )
    share_text = "Join ZEDOX and earn premium methods"
    share_url = f"https://t.me/share/url?url={link}&text={share_text.replace(' ', '%20')}"
    kb = InlineKeyboardMarkup(row_width=2)
    kb.row(
        InlineKeyboardButton("📤 Share Link", url=share_url),
        InlineKeyboardButton("💰 My Points", callback_data="open_points_card"),
    )
    raw_bot.send_message(uid, text, reply_markup=kb, disable_web_page_preview=True)

@bot.message_handler(func=lambda m: m.text == "🎁 REFERRAL")
@force_join_handler
def referral_cmd(m):
    send_referral_card(m.from_user.id)

@bot.callback_query_handler(func=lambda c: c.data == "open_referral_card")
def open_referral_card_callback(c):
    bot.answer_callback_query(c.id)
    send_referral_card(c.from_user.id)

@bot.callback_query_handler(func=lambda c: c.data == "open_points_card")
def open_points_card_callback(c):
    bot.answer_callback_query(c.id)
    fake = type("M", (), {"from_user": c.from_user})()
    points_cmd.__wrapped__(fake) if hasattr(points_cmd, "__wrapped__") else points_cmd(fake)

# =========================
# 🏆 REDEEM CODE
# =========================
@bot.message_handler(func=lambda m: m.text == "🏆 REDEEM")
@force_join_handler
def redeem_cmd(m):
    msg = bot.send_message(m.from_user.id, "🎫 **Enter code:**", parse_mode="Markdown")
    bot.register_next_step_handler(msg, redeem_code)

def redeem_code(m):
    uid = m.from_user.id
    user = User(uid)
    code = m.text.strip().upper()
    
    success, pts, reason = codesys.redeem(code, user)
    
    if success:
        bot.send_message(uid, f"✅ **Redeemed!**\n\n+{pts} points\n💰 Balance: {user.points()}", parse_mode="Markdown")
    else:
        messages = {
            "invalid": "❌ Invalid code!",
            "already_used": "❌ Code already used!",
            "already_used_by_user": "❌ You already used this code!",
            "expired": "❌ Code expired!",
            "max_uses_reached": "❌ Max uses reached!"
        }
        bot.send_message(uid, messages.get(reason, "❌ Invalid code!"), parse_mode="Markdown")

# =========================
# 🆔 CHAT ID
# =========================
@bot.message_handler(func=lambda m: m.text == "🆔 CHAT ID")
@force_join_handler
def chatid_cmd(m):
    uid = m.from_user.id
    user = User(uid)
    
    bot.send_message(uid, f"🆔 **Your ID:** `{uid}`\n\n💰 Points: {user.points()}\n⭐ VIP: {'✅' if user.is_vip() else '❌'}\n👥 Referrals: {user.get_refs_count()}", parse_mode="Markdown")

# =========================
# ⭐ BUY VIP
# =========================
@bot.message_handler(func=lambda m: m.text == "⭐ BUY VIP")
@force_join_handler
def buy_vip_button(m):
    uid = m.from_user.id
    user = User(uid)
    cfg = get_cached_config()
    
    if user.is_vip():
        bot.send_message(uid, "✅ **You are VIP!**\n\n💰 Points: {}".format(user.points()), parse_mode="Markdown")
        return
    
    vip_msg = cfg.get("vip_msg", "💎 Buy VIP!")
    vip_price_usd = cfg.get("vip_price", 50)
    vip_price_points = cfg.get("vip_points_price", 5000)
    vip_contact = cfg.get("vip_contact")
    
    binance_address = cfg.get("binance_address", "")
    binance_coin = cfg.get("binance_coin", "USDT")
    binance_network = cfg.get("binance_network", "TRC20")
    binance_memo = cfg.get("binance_memo", "")
    
    message = f"💎 **VIP**\n\n{vip_msg}\n\n💰 Price:\n• ${vip_price_usd} USD\n• {vip_price_points} points\n\n"
    
    if binance_address:
        message += f"💳 **Binance:**\nCoin: {binance_coin}\nNetwork: {binance_network}\nAddress: `{binance_address}`\n"
        if binance_memo:
            message += f"Memo: `{binance_memo}`\n"
        message += f"Amount: ${vip_price_usd}\n\n"
    
    message += f"✨ Benefits:\n• All VIP methods\n• Priority support\n• No points needed\n\n"
    
    if vip_contact:
        message += f"📞 Contact: {vip_contact}\n"
    
    message += f"\n🆔 ID: `{uid}`\n💰 Points: {user.points()}"
    
    kb = InlineKeyboardMarkup()
    if user.points() >= vip_price_points:
        kb.add(InlineKeyboardButton(f"⭐ Buy with {vip_price_points} pts", callback_data="buy_vip_points"))
    if vip_contact:
        if vip_contact.startswith("http"):
            kb.add(InlineKeyboardButton("📞 Contact", url=vip_contact))
        elif vip_contact.startswith("@"):
            kb.add(InlineKeyboardButton("📞 Contact", url=f"https://t.me/{vip_contact.replace('@', '')}"))
    
    bot.send_message(uid, message, reply_markup=kb if kb.keyboard else None, parse_mode="Markdown")

# =========================
# ⚙️ ADMIN PANEL (SHORTENED FOR SPEED)
# =========================
def admin_menu():
    kb = ReplyKeyboardMarkup(resize_keyboard=True)

    kb.row("📦 Upload FREE", "💎 Upload VIP")
    kb.row("🗑 Delete Folder", "✏️ Edit Price")
    kb.row("✏️ Edit Name", "📝 Edit Content")
    kb.row("🔀 Move Folder", "⏳ Expire Method")
    kb.row("✅ Restore Method", "👑 Add VIP")
    kb.row("👑 Remove VIP", "💰 Give Points")
    kb.row("🎫 Generate Codes", "📊 View Codes")
    kb.row("📦 Points Packages", "👥 Admin Management")
    kb.row("📞 Set Contacts", "⚙️ VIP Settings")
    kb.row("💳 Payment Methods", "🏦 Binance Settings")
    kb.row("📸 Screenshot", "🔘 Button Manager")
    kb.row("🙈 Hide Button", "👁 Show Button")
    kb.row("📢 Force Join", "👥 Join Notifications")
    kb.row("⚙️ Settings")
    kb.row("📊 Stats", "📢 Broadcast")
    kb.row("🔔 Notify", "🛡 Group Management")
    kb.row("📊 Leaderboard")
    kb.row("🔎 Search", "📣 Auto Posts")
    kb.row("📥 Auto Import", "⏳ Pending Methods")
    kb.row("📌 Pin Methods", "📝 Edit Methods List")
    kb.row("📣 Channel Approvals", "📨 Group Messenger")
    kb.row("🧾 Logs")
    kb.row("💾 Backup/Export")
    kb.row("❌ Exit")

    return kb

@bot.message_handler(func=lambda m: m.text == "⚙️ ADMIN PANEL" and is_admin(m.from_user.id))
def open_admin(m):
    bot.send_message(m.from_user.id, "⚙️ **Admin Panel**", reply_markup=admin_menu(), parse_mode="Markdown")

@bot.message_handler(func=lambda m: m.text == "❌ Exit" and is_admin(m.from_user.id))
def exit_admin(m):
    bot.send_message(m.from_user.id, "Exited", reply_markup=main_menu(m.from_user.id))

# =========================
# 📊 LEADERBOARD (NEW FEATURE)
# =========================
@bot.message_handler(func=lambda m: m.text == "📊 Leaderboard" and is_admin(m.from_user.id))
def leaderboard_menu(m):
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("🏆 Top Referrals", callback_data="top_referrals"),
        InlineKeyboardButton("💰 Top Points", callback_data="top_points"),
        InlineKeyboardButton("⭐ Top Earners", callback_data="top_earned")
    )
    bot.send_message(m.from_user.id, "📊 **Leaderboard**\n\nSelect leaderboard type:", reply_markup=kb, parse_mode="Markdown")

@bot.callback_query_handler(func=lambda c: c.data == "top_referrals")
def top_referrals_cb(c):
    users = list(users_col.find({}).sort("refs", -1).limit(30))
    text = "🏆 **TOP 30 USERS BY REFERRALS** 🏆\n\n"
    
    for i, user in enumerate(users, 1):
        username = user.get("username") or f"User_{user['_id'][:6]}"
        refs = user.get("refs", 0)
        is_vip = "👑" if user.get("vip", False) else "📌"
        text += f"{i}. {is_vip} <code>{username}</code> → {refs} referrals\n"
    
    if not users:
        text += "No users found!"
    
    bot.edit_message_text(text, c.from_user.id, c.message.message_id, parse_mode="HTML")
    bot.answer_callback_query(c.id)

@bot.callback_query_handler(func=lambda c: c.data == "top_points")
def top_points_cb(c):
    users = list(users_col.find({}).sort("points", -1).limit(30))
    text = "💰 **TOP 30 USERS BY POINTS** 💰\n\n"
    
    for i, user in enumerate(users, 1):
        username = user.get("username") or f"User_{user['_id'][:6]}"
        points = user.get("points", 0)
        is_vip = "👑" if user.get("vip", False) else "📌"
        text += f"{i}. {is_vip} <code>{username}</code> → {points:,} pts\n"
    
    if not users:
        text += "No users found!"
    
    bot.edit_message_text(text, c.from_user.id, c.message.message_id, parse_mode="HTML")
    bot.answer_callback_query(c.id)

@bot.callback_query_handler(func=lambda c: c.data == "top_earned")
def top_earned_cb(c):
    users = list(users_col.find({}).sort("total_points_earned", -1).limit(30))
    text = "⭐ **TOP 30 USERS BY POINTS EARNED** ⭐\n\n"
    
    for i, user in enumerate(users, 1):
        username = user.get("username") or f"User_{user['_id'][:6]}"
        earned = user.get("total_points_earned", 0)
        is_vip = "👑" if user.get("vip", False) else "📌"
        text += f"{i}. {is_vip} <code>{username}</code> → {earned:,} pts earned\n"
    
    if not users:
        text += "No users found!"
    
    bot.edit_message_text(text, c.from_user.id, c.message.message_id, parse_mode="HTML")
    bot.answer_callback_query(c.id)

# =========================
# 📤 UPLOAD SYSTEM (FAST)
# =========================
upload_sessions = {}

def start_upload(uid, cat, is_service=False):
    upload_sessions[uid] = {"cat": cat, "service": is_service, "files": [], "step": "name"}
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row("📄 Text", "📁 Files")
    kb.row("/cancel")
    msg = bot.send_message(uid, f"📤 **Upload to {cat.upper()}**\n\nChoose:", reply_markup=kb, parse_mode="Markdown")
    bot.register_next_step_handler(msg, lambda m: upload_type_choice(m, cat, is_service))

def upload_type_choice(m, cat, is_service):
    if m.text == "/cancel":
        upload_sessions.pop(m.from_user.id, None)
        bot.send_message(m.from_user.id, "❌ Cancelled", reply_markup=admin_menu())
        return
    
    if m.text == "📄 Text":
        msg = bot.send_message(m.from_user.id, "📝 **Folder name:**", parse_mode="Markdown")
        bot.register_next_step_handler(msg, lambda x: upload_text_name(x, cat, is_service))
    elif m.text == "📁 Files":
        kb = ReplyKeyboardMarkup(resize_keyboard=True)
        kb.row("/done", "/cancel")
        msg = bot.send_message(m.from_user.id, f"📤 **Upload files**\n\nSend files, /done when finished:", reply_markup=kb, parse_mode="Markdown")
        bot.register_next_step_handler(msg, lambda x: upload_file_step(x, cat, m.from_user.id, [], is_service))
    else:
        bot.send_message(m.from_user.id, "❌ Invalid", reply_markup=admin_menu())

def upload_text_name(m, cat, is_service):
    name = m.text
    msg = bot.send_message(m.from_user.id, "💰 **Price (0 = free):**", parse_mode="Markdown")
    bot.register_next_step_handler(msg, lambda x: upload_text_price(x, cat, name, is_service))

def upload_text_price(m, cat, name, is_service):
    try:
        price = int(m.text)
        msg = bot.send_message(m.from_user.id, "📝 **Content:**", parse_mode="Markdown")
        bot.register_next_step_handler(msg, lambda x: upload_text_save(x, cat, name, price, is_service))
    except:
        bot.send_message(m.from_user.id, "❌ Invalid price!")

def upload_text_save(m, cat, name, price, is_service):
    text_content = m.text
    number = fs.add(cat, name, [], price, text_content=text_content)
    send_method_notification("uploaded", fs.get_by_number(number) or {"cat":cat,"name":name,"number":number,"price":price})
    
    if is_service:
        folder = fs.get_one(cat, name)
        if folder:
            folders_col.update_one({"_id": folder["_id"]}, {"$set": {"service_msg": text_content}})
    
    bot.send_message(m.from_user.id, f"✅ Added!\n📌 #{number}\n📂 {name}\n💰 {price} pts", reply_markup=admin_menu(), parse_mode="Markdown")
    upload_sessions.pop(m.from_user.id, None)

def upload_file_step(m, cat, uid, files, is_service):
    if m.text == "/cancel":
        upload_sessions.pop(uid, None)
        bot.send_message(uid, "❌ Cancelled", reply_markup=admin_menu())
        return
    
    if m.text == "/done":
        if not files:
            bot.send_message(uid, "❌ No files!")
            return
        msg = bot.send_message(uid, "📝 **Folder name:**", parse_mode="Markdown")
        bot.register_next_step_handler(msg, lambda x: upload_file_name(x, cat, files, is_service))
        return
    
    if m.content_type in ["document", "photo", "video"]:
        files.append({"chat": m.chat.id, "msg": m.message_id, "type": m.content_type})
        bot.send_message(uid, f"✅ Saved ({len(files)} files)")
    
    bot.register_next_step_handler(m, lambda x: upload_file_step(x, cat, uid, files, is_service))

def upload_file_name(m, cat, files, is_service):
    name = m.text
    msg = bot.send_message(m.from_user.id, "💰 **Price (0 = free):**", parse_mode="Markdown")
    bot.register_next_step_handler(msg, lambda x: upload_file_save(x, cat, name, files, is_service))

def upload_file_save(m, cat, name, files, is_service):
    try:
        price = int(m.text)
        number = fs.add(cat, name, files, price)
        send_method_notification("uploaded", fs.get_by_number(number) or {"cat":cat,"name":name,"number":number,"price":price})
        
        if is_service:
            msg = bot.send_message(m.from_user.id, "📝 **Service message:**", parse_mode="Markdown")
            bot.register_next_step_handler(msg, lambda x: service_msg_save(x, cat, name, number, price, files))
        else:
            bot.send_message(m.from_user.id, f"✅ Uploaded!\n📌 #{number}\n📂 {name}\n💰 {price} pts\n📁 {len(files)} files", reply_markup=admin_menu(), parse_mode="Markdown")
            upload_sessions.pop(m.from_user.id, None)
    except:
        bot.send_message(m.from_user.id, "❌ Invalid price!")

def service_msg_save(m, cat, name, number, price, files):
    service_msg = m.text
    folder = fs.get_one(cat, name)
    if folder:
        folders_col.update_one({"_id": folder["_id"]}, {"$set": {"service_msg": service_msg}})
    
    bot.send_message(m.from_user.id, f"✅ Service added!\n📌 #{number}\n📂 {name}\n💰 {price} pts\n📁 {len(files)} files", reply_markup=admin_menu(), parse_mode="Markdown")
    upload_sessions.pop(m.from_user.id, None)

@bot.message_handler(func=lambda m: m.text in ["📦 Upload FREE", "💎 Upload VIP"] and is_admin(m.from_user.id))
def upload_handler(m):
    cats = {"📦 Upload FREE": "free", "💎 Upload VIP": "vip"}
    start_upload(m.from_user.id, cats[m.text], False)


# =========================
# 🔀 MOVE FOLDER
# =========================
_move_state = {}

def _method_select_keyboard(prefix, category=None, include_expired=True):
    query = {} if category is None else {"cat": category}
    if not include_expired:
        query["expired"] = {"$ne": True}
    rows = list(folders_col.find(query).sort([("pinned", -1), ("created_at", -1)]))
    kb = InlineKeyboardMarkup(row_width=1)
    for row in rows[:80]:
        label = f"{str(row.get('cat','')).upper()} • {row.get('name','Unnamed')}"
        if row.get("parent"):
            label += f" / {row.get('parent')}"
        kb.add(InlineKeyboardButton(label[:62], callback_data=f"{prefix}|{row['_id']}"))
    return kb

@bot.message_handler(func=lambda m: m.text == "🔀 Move Folder" and is_admin(m.from_user.id))
def move_folder_start(m):
    raw_bot.send_message(m.from_user.id, "🔀 MOVE METHOD / FOLDER\n\nSelect what you want to move:", reply_markup=_method_select_keyboard("moveselect"))

@bot.callback_query_handler(func=lambda c: c.data.startswith("moveselect|"))
def move_select_cb(c):
    if not is_admin(c.from_user.id): return
    from bson import ObjectId
    row = folders_col.find_one({"_id": ObjectId(c.data.split("|",1)[1])})
    if not row: return bot.answer_callback_query(c.id,"Not found",True)
    _move_state[c.from_user.id] = str(row["_id"])
    candidates = list(folders_col.find({"cat": row.get("cat"), "_id": {"$ne": row["_id"]}}).sort("name",1))
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("🏠 Move to Main Level", callback_data="movedest|root"))
    for dest in candidates[:80]:
        kb.add(InlineKeyboardButton(f"📁 {dest.get('name','Unnamed')}"[:62], callback_data=f"movedest|{dest['_id']}"))
    raw_bot.send_message(c.from_user.id, f"Selected: {row.get('name')}\n\nChoose the destination:", reply_markup=kb)
    bot.answer_callback_query(c.id)

@bot.callback_query_handler(func=lambda c: c.data.startswith("movedest|"))
def move_dest_cb(c):
    if not is_admin(c.from_user.id): return
    from bson import ObjectId
    source_id = _move_state.pop(c.from_user.id, None)
    if not source_id: return bot.answer_callback_query(c.id,"Session expired",True)
    source = folders_col.find_one({"_id": ObjectId(source_id)})
    if not source: return bot.answer_callback_query(c.id,"Source not found",True)
    dest_value = c.data.split("|",1)[1]
    parent = None
    if dest_value != "root":
        dest = folders_col.find_one({"_id": ObjectId(dest_value)})
        if not dest: return bot.answer_callback_query(c.id,"Destination not found",True)
        parent = dest.get("name")
    folders_col.update_one({"_id": source["_id"]}, {"$set": {"parent": parent, "updated_at": now_ts()}})
    admin_success(c.from_user.id, f"Moved {source.get('name')} to {parent or 'Main Level'}")
    bot.answer_callback_query(c.id,"Moved")

@bot.message_handler(func=lambda m: m.text in ("⏳ Expire Method", "✅ Restore Method") and is_admin(m.from_user.id))
def expire_restore_menu(m):
    mode = "expire" if m.text.startswith("⏳") else "restore"
    query = {"expired": {"$ne": True}} if mode == "expire" else {"expired": True}
    rows = list(folders_col.find(query).sort([("cat", 1), ("pinned", -1), ("name", 1)]))
    kb = InlineKeyboardMarkup(row_width=1)
    for row in rows[:100]:
        icon = "⏳" if mode == "expire" else "✅"
        kb.add(InlineKeyboardButton(
            f"{icon} {str(row.get('cat', '')).upper()} • {row.get('name', 'Unnamed')}"[:62],
            callback_data=f"methodstatusconfirm|{mode}|{row['_id']}",
        ))
    raw_bot.send_message(
        m.from_user.id,
        ("⛔ Select the method to expire:" if mode == "expire" else "✅ Select the method to restore:")
        if rows else "No methods are available for this action.",
        reply_markup=kb if rows else None,
    )


@bot.callback_query_handler(func=lambda c: c.data.startswith("methodstatusconfirm|"))
def method_status_confirm_cb(c):
    if not is_admin(c.from_user.id):
        return
    from bson import ObjectId
    try:
        _, mode, oid = c.data.split("|", 2)
        row = folders_col.find_one({"_id": ObjectId(oid)})
        if not row:
            return bot.answer_callback_query(c.id, "Method not found", True)
        kb = InlineKeyboardMarkup(row_width=2)
        kb.add(
            InlineKeyboardButton(
                "⛔ Yes, Expire" if mode == "expire" else "✅ Yes, Restore",
                callback_data=f"methodstatusapply|{mode}|{oid}",
            ),
            InlineKeyboardButton("❌ Cancel", callback_data="methodstatuscancel"),
        )
        raw_bot.send_message(
            c.from_user.id,
            f"{'⛔ EXPIRE METHOD' if mode == 'expire' else '✅ RESTORE METHOD'}\n\n{row.get('name')}\n\n"
            + ("Users will not be able to buy, open, or receive this method." if mode == "expire" else "Users will be able to access this method again."),
            reply_markup=kb,
        )
        bot.answer_callback_query(c.id)
    except Exception as exc:
        admin_error(c.from_user.id, exc)


@bot.callback_query_handler(func=lambda c: c.data.startswith("methodstatusapply|"))
def method_status_apply_cb(c):
    if not is_admin(c.from_user.id):
        return
    from bson import ObjectId
    try:
        _, mode, oid = c.data.split("|", 2)
        expired = mode == "expire"
        result = folders_col.update_one(
            {"_id": ObjectId(oid)},
            {"$set": {
                "expired": expired,
                "active": not expired,
                "updated_at": now_ts(),
                "expired_at": now_ts() if expired else None,
                "expired_by": c.from_user.id if expired else None,
            }},
        )
        if result.matched_count != 1:
            raise ValueError("Method was not found or could not be updated")
        row = folders_col.find_one({"_id": ObjectId(oid)})
        if not row or bool(row.get("expired")) != expired:
            raise ValueError("Expiry status verification failed")
        send_method_notification("expired" if expired else "restored", row)
        admin_success(c.from_user.id, f"{row.get('name')} {'expired' if expired else 'restored'} successfully")
        bot.answer_callback_query(c.id, "Updated")
    except Exception as exc:
        admin_error(c.from_user.id, exc)


@bot.callback_query_handler(func=lambda c: c.data == "methodstatuscancel")
def method_status_cancel_cb(c):
    if is_admin(c.from_user.id):
        bot.answer_callback_query(c.id, "Cancelled")
        raw_bot.send_message(c.from_user.id, "❌ Cancelled", reply_markup=admin_menu())

# =========================
# 🗂 FOLDER ACTION PICKER
# =========================
_folder_admin_state = {}

def folder_action_keyboard(action, page=0, per_page=20):
    rows = list(folders_col.find({}, {"number":1,"name":1,"cat":1,"parent":1,"price":1}).sort([("cat",1),("number",1)]))
    kb = InlineKeyboardMarkup(row_width=1)
    start = page * per_page
    for f in rows[start:start+per_page]:
        label = f"[{f.get('number','?')}] {str(f.get('cat','')).upper()} • {f.get('name')}"
        if f.get('parent'): label += f" / {f.get('parent')}"
        kb.add(InlineKeyboardButton(label[:60], callback_data=f"folderact|{action}|{f.get('number')}"))
    nav=[]
    if page>0: nav.append(InlineKeyboardButton("⬅️", callback_data=f"folderpage|{action}|{page-1}"))
    if start+per_page<len(rows): nav.append(InlineKeyboardButton("➡️", callback_data=f"folderpage|{action}|{page+1}"))
    if nav: kb.row(*nav)
    return kb

def show_folder_action(uid, action, title):
    kb=folder_action_keyboard(action)
    if not kb.keyboard:
        return bot.send_message(uid,"❌ No methods/folders found.",reply_markup=admin_menu())
    bot.send_message(uid,title,reply_markup=kb,parse_mode="Markdown")

@bot.message_handler(func=lambda m: m.text == "🗑 Delete Folder" and is_admin(m.from_user.id))
def del_start(m): show_folder_action(m.from_user.id,"delete","🗑 **Select a method/folder to delete:**")

@bot.message_handler(func=lambda m: m.text == "✏️ Edit Price" and is_admin(m.from_user.id))
def edit_price_start(m): show_folder_action(m.from_user.id,"price","✏️ **Select a method/folder to edit price:**")

@bot.message_handler(func=lambda m: m.text == "✏️ Edit Name" and is_admin(m.from_user.id))
def edit_name_start(m): show_folder_action(m.from_user.id,"name","✏️ **Select a method/folder to rename:**")

@bot.message_handler(func=lambda m: m.text == "📝 Edit Content" and is_admin(m.from_user.id))
def edit_content_start(m): show_folder_action(m.from_user.id,"content","📝 **Select a method/folder to edit content:**")

@bot.callback_query_handler(func=lambda c:c.data.startswith("folderpage|"))
def folder_page_cb(c):
    if not is_admin(c.from_user.id): return bot.answer_callback_query(c.id,"Admin only",True)
    _,action,page=c.data.split("|")
    bot.edit_message_reply_markup(c.from_user.id,c.message.message_id,reply_markup=folder_action_keyboard(action,int(page)))
    bot.answer_callback_query(c.id)

@bot.callback_query_handler(func=lambda c:c.data.startswith("folderact|"))
def folder_action_cb(c):
    if not is_admin(c.from_user.id): return bot.answer_callback_query(c.id,"Admin only",True)
    try:
        _,action,num=c.data.split("|"); folder=fs.get_by_number(int(num))
        if not folder: raise ValueError("Folder not found")
        _folder_admin_state[c.from_user.id]={"action":action,"number":int(num)}
        if action=="delete":
            kb=InlineKeyboardMarkup(row_width=2)
            kb.add(InlineKeyboardButton("✅ Confirm Delete",callback_data=f"folderconfirm|delete|{num}"),InlineKeyboardButton("❌ Cancel",callback_data="folderconfirm|cancel|0"))
            bot.send_message(c.from_user.id,f"⚠️ Delete **[{num}] {folder['name']}** from **{folder['cat'].upper()}**?\nThis also deletes its subfolders.",reply_markup=kb,parse_mode="Markdown")
        elif action=="price":
            msg=bot.send_message(c.from_user.id,f"Current price: `{folder.get('price',0)}`\nSend the new price:",parse_mode="Markdown");bot.register_next_step_handler(msg,folder_price_step)
        elif action=="name":
            msg=bot.send_message(c.from_user.id,f"Current name: **{folder['name']}**\nSend the new name:",parse_mode="Markdown");bot.register_next_step_handler(msg,folder_name_step)
        else:
            edit_sessions[c.from_user.id]={"cat":folder['cat'],"name":folder['name'],"parent":folder.get('parent'),"number":int(num)}
            kb=InlineKeyboardMarkup(row_width=2);kb.add(InlineKeyboardButton("📝 Text",callback_data="edit_text"),InlineKeyboardButton("📁 Files",callback_data="edit_files"),InlineKeyboardButton("❌ Cancel",callback_data="edit_cancel"))
            bot.send_message(c.from_user.id,f"📝 **Edit [{num}] {folder['name']}**\nWhat do you want to update?",reply_markup=kb,parse_mode="Markdown")
        bot.answer_callback_query(c.id)
    except Exception as exc: bot.answer_callback_query(c.id,str(exc),True)

@bot.callback_query_handler(func=lambda c:c.data.startswith("folderconfirm|"))
def folder_confirm_cb(c):
    if not is_admin(c.from_user.id): return
    try:
        _,action,num=c.data.split("|")
        if action=="cancel":
            bot.edit_message_text("❌ Cancelled",c.from_user.id,c.message.message_id);return bot.answer_callback_query(c.id)
        folder=fs.get_by_number(int(num))
        if not folder: raise ValueError("Folder no longer exists")
        ok=fs.delete(folder['cat'],folder['name'],folder.get('parent'))
        if not ok: raise ValueError("Delete failed")
        bot.edit_message_text(f"✅ Process Complete\nDeleted: [{num}] {folder['name']}",c.from_user.id,c.message.message_id)
        bot.answer_callback_query(c.id,"Deleted")
    except Exception as exc: admin_error(c.from_user.id,exc);bot.answer_callback_query(c.id,"Failed",True)

def folder_price_step(m):
    try:
        st=_folder_admin_state.pop(m.from_user.id,None); folder=fs.get_by_number(st['number']) if st else None
        if not folder: raise ValueError("Session expired or folder missing")
        price=int((m.text or '').strip())
        if price<0: raise ValueError("Price cannot be negative")
        folders_col.update_one({"_id":folder["_id"]},{"$set":{"price":price}})
        folder['price']=price;send_method_notification("updated",folder);admin_success(m.from_user.id,f"Price updated to {price} points")
    except Exception as exc: admin_error(m.from_user.id,exc)

def folder_name_step(m):
    try:
        st=_folder_admin_state.pop(m.from_user.id,None); folder=fs.get_by_number(st['number']) if st else None
        if not folder: raise ValueError("Session expired or folder missing")
        new=(m.text or '').strip()
        if not new or len(new)>100: raise ValueError("Name must be 1-100 characters")
        old=folder['name']; folders_col.update_one({"_id":folder["_id"]},{"$set":{"name":new}});folders_col.update_many({"cat":folder['cat'],"parent":old},{"$set":{"parent":new}})
        folder['name']=new;send_method_notification("updated",folder);admin_success(m.from_user.id,f"Renamed to {new}")
    except Exception as exc: admin_error(m.from_user.id,exc)

edit_sessions = {}

@bot.callback_query_handler(func=lambda c: c.data == "edit_text")
def edit_text_cb(c):
    uid = c.from_user.id
    if uid not in edit_sessions:
        bot.answer_callback_query(c.id, "Session expired!")
        return
    
    s = edit_sessions[uid]
    folder = fs.get_one(s["cat"], s["name"])
    current = folder.get("text_content", "No content")[:200]
    msg = bot.send_message(uid, f"📝 **Current:**\n{current}\n\nSend NEW text:", parse_mode="Markdown")
    bot.register_next_step_handler(msg, save_edit_text)
    bot.answer_callback_query(c.id)

def save_edit_text(m):
    uid = m.from_user.id
    if uid not in edit_sessions:
        bot.send_message(uid, "Session expired!", reply_markup=admin_menu())
        return
    
    s = edit_sessions[uid]
    fs.edit_content(s["cat"], s["name"], "text", m.text, s.get("parent"))
    folder=fs.get_by_number(s.get("number")) or fs.get_one(s["cat"],s["name"],s.get("parent")); send_method_notification("updated",folder or s)
    bot.send_message(uid, f"✅ Text updated!", reply_markup=admin_menu())
    edit_sessions.pop(uid, None)

@bot.callback_query_handler(func=lambda c: c.data == "edit_files")
def edit_files_cb(c):
    uid = c.from_user.id
    if uid not in edit_sessions:
        bot.answer_callback_query(c.id, "Session expired!")
        return
    
    edit_sessions[uid]["new_files"] = []
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add("/done", "/cancel")
    msg = bot.send_message(uid, "📁 Send NEW files\n/done when finished:", reply_markup=kb, parse_mode="Markdown")
    bot.register_next_step_handler(msg, process_edit_files)
    bot.answer_callback_query(c.id)

def process_edit_files(m):
    uid = m.from_user.id
    if m.text == "/cancel":
        edit_sessions.pop(uid, None)
        bot.send_message(uid, "❌ Cancelled", reply_markup=admin_menu())
        return
    
    if m.text == "/done":
        if uid not in edit_sessions:
            bot.send_message(uid, "Session expired!")
            return
        s = edit_sessions[uid]
        if not s.get("new_files"):
            bot.send_message(uid, "❌ No files!")
            return
        fs.edit_content(s["cat"], s["name"], "files", s["new_files"], s.get("parent"))
        folder=fs.get_by_number(s.get("number")) or fs.get_one(s["cat"],s["name"],s.get("parent")); send_method_notification("updated",folder or s)
        bot.send_message(uid, f"✅ {len(s['new_files'])} file(s) updated!", reply_markup=admin_menu())
        edit_sessions.pop(uid, None)
        return
    
    if m.content_type in ["document", "photo", "video"]:
        edit_sessions[uid]["new_files"].append({"chat": m.chat.id, "msg": m.message_id, "type": m.content_type})
        bot.send_message(uid, f"✅ Saved ({len(edit_sessions[uid]['new_files'])} files)")
    else:
        bot.send_message(uid, "❌ Send documents, photos, or videos!")
    bot.register_next_step_handler(m, process_edit_files)

@bot.callback_query_handler(func=lambda c: c.data == "edit_cancel")
def edit_cancel_cb(c):
    edit_sessions.pop(c.from_user.id, None)
    bot.edit_message_text("❌ Cancelled", c.from_user.id, c.message.message_id)
    bot.send_message(c.from_user.id, "Returning...", reply_markup=admin_menu())
    bot.answer_callback_query(c.id)

# =========================
# 👑 ADD VIP
# =========================
@bot.message_handler(func=lambda m: m.text == "👑 Add VIP" and is_admin(m.from_user.id))
def add_vip_start(m):
    msg = bot.send_message(m.from_user.id, "👑 **Add VIP**\n\nSend user ID or @username:", parse_mode="Markdown")
    bot.register_next_step_handler(msg, add_vip_process)

def add_vip_process(m):
    inp = m.text.strip()
    if inp.startswith("@"):
        try:
            target = bot.get_chat(inp).id
        except:
            bot.send_message(m.from_user.id, "❌ User not found!")
            return
    else:
        try:
            target = int(inp)
        except:
            bot.send_message(m.from_user.id, "❌ Invalid ID!")
            return
    
    u = User(target)
    if u.is_vip():
        bot.send_message(m.from_user.id, "⚠️ Already VIP!")
        return
    
    u.make_vip(get_config().get("vip_duration_days", 30))
    bot.send_message(m.from_user.id, f"✅ User {target} is now VIP!")
    try:
        bot.send_message(target, "🎉 **You are now VIP!** 🎉\n\nAccess all VIP methods!", parse_mode="Markdown")
    except:
        pass

# =========================
# 👑 REMOVE VIP
# =========================
@bot.message_handler(func=lambda m: m.text == "👑 Remove VIP" and is_admin(m.from_user.id))
def remove_vip_start(m):
    msg = bot.send_message(m.from_user.id, "👑 **Remove VIP**\n\nSend user ID or @username:", parse_mode="Markdown")
    bot.register_next_step_handler(msg, remove_vip_process)

def remove_vip_process(m):
    inp = m.text.strip()
    if inp.startswith("@"):
        try:
            target = bot.get_chat(inp).id
        except:
            bot.send_message(m.from_user.id, "❌ User not found!")
            return
    else:
        try:
            target = int(inp)
        except:
            bot.send_message(m.from_user.id, "❌ Invalid ID!")
            return
    
    u = User(target)
    if not u.is_vip():
        bot.send_message(m.from_user.id, "⚠️ Not VIP!")
        return
    
    u.remove_vip()
    bot.send_message(m.from_user.id, f"✅ VIP removed from {target}!")
    try:
        bot.send_message(target, "⚠️ VIP status removed.", parse_mode="Markdown")
    except:
        pass

# =========================
# 💰 GIVE POINTS (FIXED - FULLY WORKING)
# =========================
@bot.message_handler(func=lambda m: m.text == "💰 Give Points" and is_admin(m.from_user.id))
def give_points_start(m):
    msg = bot.send_message(m.from_user.id, 
        "💰 **Give Points**\n\n"
        "Send: `user_id points`\n\n"
        "Example: `7712834912 200`\n\n"
        "*User must have started the bot first*",
        parse_mode="Markdown")
    bot.register_next_step_handler(msg, give_points_process)

def give_points_process(m):
    admin_id = m.from_user.id
    try:
        if not is_admin(admin_id):
            raise PermissionError("Admins only")
        parts = (m.text or "").strip().split()
        if len(parts) != 2:
            raise ValueError("Send exactly: user_id points")
        user_id_text, points_text = parts
        if not re.fullmatch(r"\\d{5,20}", user_id_text):
            raise ValueError("Invalid Telegram user ID")
        if not re.fullmatch(r"\\d{1,7}", points_text):
            raise ValueError("Points must contain digits only")
        user_id = int(user_id_text)
        amount = int(points_text)
        if not 1 <= amount <= 1_000_000:
            raise ValueError("Points must be between 1 and 1,000,000")

        reliable_users = users_col.with_options(write_concern=WriteConcern(w=1))
        before = reliable_users.find_one({"_id": str(user_id)})
        if not before:
            raise ValueError("User not found. Ask the user to send /start first")
        old_balance = int(before.get("points", 0) or 0)
        result = reliable_users.update_one(
            {"_id": str(user_id)},
            {"$inc": {"points": amount, "total_points_earned": amount}, "$set": {"last_active": time.time()}},
        )
        if result.matched_count != 1 or result.modified_count != 1:
            raise RuntimeError("Database did not update the user balance")
        after = reliable_users.find_one({"_id": str(user_id)}, {"points": 1, "username": 1}) or {}
        new_balance = int(after.get("points", old_balance + amount))

        for key in (user_id, str(user_id)):
            User._cache.pop(key, None)
            User._cache_time.pop(key, None)
        try:
            point_history_col.with_options(write_concern=WriteConcern(w=1)).insert_one({
                "user_id": str(user_id), "amount": amount, "reason": "manual_give_points",
                "admin_id": str(admin_id), "created_at": time.time(),
            })
        except Exception:
            pass

        username = after.get("username")
        user_label = f"@{username}" if username else str(user_id)
        raw_bot.send_message(
            admin_id,
            "✅  POINTS ADDED SUCCESSFULLY\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"👤 User: {user_label}\n"
            f"🆔 ID: {user_id}\n"
            f"➕ Added: {amount:,} points\n"
            f"💰 Previous: {old_balance:,}\n"
            f"💎 New balance: {new_balance:,}",
            reply_markup=admin_menu(),
        )
        try:
            raw_bot.send_message(
                user_id,
                "🎉✨  CONGRATULATIONS!  ✨🎉\n"
                "━━━━━━━━━━━━━━━━━━━━\n\n"
                f"💫 You have received {amount:,} points!\n\n"
                f"💰 Previous balance: {old_balance:,}\n"
                f"💎 New balance: {new_balance:,}\n\n"
                "🥳 Enjoy your reward and unlock more methods! 🚀",
                reply_markup=main_menu(user_id),
            )
        except Exception as notify_exc:
            raw_bot.send_message(admin_id, f"⚠️ Points were added, but notification failed: {notify_exc}")
    except Exception as exc:
        admin_error(admin_id, f"{exc}\n\nExample: 7712834912 200")

# =========================
# 🎫 GENERATE CODES (FIXED)
# =========================
@bot.message_handler(func=lambda m: m.text == "🎫 Generate Codes" and is_admin(m.from_user.id))
def gen_codes_start(m):
    msg = bot.send_message(
        m.from_user.id,
        "🎫 **Generate Codes**\n\n"
        "Send: `points count type expiry_days`\n\n"
        "Type: `single` or `multi`\n"
        "Expiry: `0` for no expiry\n\n"
        "Examples:\n"
        "`100 5 single 0`\n"
        "`250 10 multi 7`",
        parse_mode="Markdown",
    )
    bot.register_next_step_handler(msg, generate_codes_process)

def generate_codes_process(m):
    uid = m.from_user.id
    try:
        if not is_admin(uid):
            raise PermissionError("Admins only")
        parts = (m.text or "").strip().lower().split()
        if len(parts) != 4:
            raise ValueError("Send exactly: points count single|multi expiry_days")
        points, count, code_type, expiry_raw = int(parts[0]), int(parts[1]), parts[2], int(parts[3])
        if not 1 <= points <= 1_000_000:
            raise ValueError("Points must be between 1 and 1,000,000")
        if not 1 <= count <= 100:
            raise ValueError("Code count must be between 1 and 100")
        if code_type not in ("single", "multi"):
            raise ValueError("Type must be single or multi")
        if not 0 <= expiry_raw <= 3650:
            raise ValueError("Expiry must be between 0 and 3650 days")

        expiry = time.time() + expiry_raw * 86400 if expiry_raw else None
        reliable_codes = codes_col.with_options(write_concern=WriteConcern(w=1))
        generated = []
        for _ in range(count):
            for attempt in range(20):
                code = "ZEDOX" + "".join(random.choices(string.ascii_uppercase + string.digits, k=8))
                if not reliable_codes.find_one({"_id": code}, {"_id": 1}):
                    break
            doc = {
                "_id": code, "points": points, "used": False,
                "multi_use": code_type == "multi", "used_count": 0,
                "max_uses": 10 if code_type == "multi" else 1,
                "expiry": expiry, "created_at": time.time(), "used_by_users": [],
            }
            reliable_codes.insert_one(doc)
            generated.append(code)
        if len(generated) != count:
            raise RuntimeError("Some codes could not be created")

        header = (
            "✅  CODES GENERATED\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"💰 Value: {points:,} points each\n"
            f"🎟 Quantity: {count}\n"
            f"🔁 Type: {code_type.upper()}\n"
            f"⏳ Expiry: {expiry_raw if expiry_raw else 'No expiry'}\n\n"
        )
        # Split safely to stay under Telegram's message limit.
        chunks, current = [], header
        for code in generated:
            line = code + "\n"
            if len(current) + len(line) > 3900:
                chunks.append(current.rstrip())
                current = line
            else:
                current += line
        if current.strip():
            chunks.append(current.rstrip())
        for i, chunk in enumerate(chunks):
            raw_bot.send_message(uid, chunk, reply_markup=admin_menu() if i == len(chunks)-1 else None)
    except Exception as exc:
        admin_error(uid, f"{exc}\n\nExample: 100 5 single 0")

# =========================
# 📊 VIEW CODES
# =========================
@bot.message_handler(func=lambda m: m.text == "📊 View Codes" and is_admin(m.from_user.id))
def view_codes(m):
    codes = codesys.get_all_codes()
    if not codes:
        bot.send_message(m.from_user.id, "📊 No codes!")
        return
    
    total, used, unused, multi = codesys.get_stats()
    text = f"📊 **Codes**\n\nTotal: {total}\nUsed: {used}\nUnused: {unused}\nMulti: {multi}\n\n"
    
    unused_codes = [c for c in codes if not c.get("used", False)][:5]
    if unused_codes:
        text += "**Recent:**\n"
        for c in unused_codes:
            text += f"• `{c['_id']}` - {c['points']} pts\n"
    
    bot.send_message(m.from_user.id, text, parse_mode="Markdown")

# =========================
# 📦 POINTS PACKAGES
# =========================
@bot.message_handler(func=lambda m: m.text == "📦 Points Packages" and is_admin(m.from_user.id))
def packages_cmd(m):
    pkgs = get_points_packages()
    text = "📦 **Points Packages**\n\n"
    for i, p in enumerate(pkgs, 1):
        status = "✅" if p.get("active", True) else "❌"
        text += f"{i}. {status} {p['points']} pts - ${p['price']}"
        if p.get("bonus", 0) > 0:
            text += f" (+{p['bonus']})"
        text += "\n"
    text += "\n/addpackage pts price bonus\n/editpackage num pts price bonus\n/togglepackage num\n/delpackage num"
    bot.send_message(m.from_user.id, text, parse_mode="Markdown")

@bot.message_handler(commands=["addpackage", "editpackage", "togglepackage", "delpackage"])
def pkg_commands(m):
    if not is_admin(m.from_user.id):
        return
    
    cmd = m.text.split()[0][1:]
    pkgs = get_points_packages()
    
    try:
        if cmd == "addpackage":
            _, pts, price, bonus = m.text.split()
            pkgs.append({"points": int(pts), "price": int(price), "bonus": int(bonus), "active": True})
            save_points_packages(pkgs)
            bot.send_message(m.from_user.id, f"✅ Added: {pts} pts for ${price}")
        elif cmd == "editpackage":
            _, num, pts, price, bonus = m.text.split()
            num = int(num) - 1
            if 0 <= num < len(pkgs):
                pkgs[num].update({"points": int(pts), "price": int(price), "bonus": int(bonus)})
                save_points_packages(pkgs)
                bot.send_message(m.from_user.id, f"✅ Package {num+1} updated!")
            else:
                bot.send_message(m.from_user.id, "❌ Invalid number!")
        elif cmd == "togglepackage":
            _, num = m.text.split()
            num = int(num) - 1
            if 0 <= num < len(pkgs):
                pkgs[num]["active"] = not pkgs[num].get("active", True)
                save_points_packages(pkgs)
                status = "activated" if pkgs[num]["active"] else "deactivated"
                bot.send_message(m.from_user.id, f"✅ Package {num+1} {status}!")
            else:
                bot.send_message(m.from_user.id, "❌ Invalid number!")
        elif cmd == "delpackage":
            _, num = m.text.split()
            num = int(num) - 1
            if 0 <= num < len(pkgs):
                removed = pkgs.pop(num)
                save_points_packages(pkgs)
                bot.send_message(m.from_user.id, f"✅ Removed: {removed['points']} pts")
            else:
                bot.send_message(m.from_user.id, "❌ Invalid number!")
    except:
        bot.send_message(m.from_user.id, f"❌ Use: /{cmd} ...")

# =========================
# 👥 ADMIN MANAGEMENT
# =========================
@bot.message_handler(func=lambda m: m.text == "👥 Admin Management" and is_admin(m.from_user.id))
def admin_management_cmd(m):
    if m.from_user.id != ADMIN_ID:
        bot.send_message(m.from_user.id, "❌ Owner only!")
        return
    
    admins = get_all_admins()
    text = "👥 **Admins**\n\n"
    for a in admins:
        owner = " 👑" if a["_id"] == ADMIN_ID else ""
        text += f"• `{a['_id']}`{owner}\n"
    text += "\n/addadmin id\n/removeadmin id\n/listadmins"
    bot.send_message(m.from_user.id, text, parse_mode="Markdown")

@bot.message_handler(commands=["addadmin", "removeadmin", "listadmins"])
def admin_commands(m):
    if m.from_user.id != ADMIN_ID:
        return
    
    cmd = m.text.split()[0][1:]
    
    if cmd == "listadmins":
        admins = get_all_admins()
        text = "👥 Admins:\n"
        for a in admins:
            text += f"• `{a['_id']}`\n"
        bot.send_message(m.from_user.id, text, parse_mode="Markdown")
        return
    
    try:
        _, uid = m.text.split()
        uid = int(uid)
        
        if cmd == "addadmin":
            if admins_col.find_one({"_id": uid}):
                bot.send_message(m.from_user.id, "❌ Already admin!")
                return
            admins_col.insert_one({"_id": uid, "added_at": time.time()})
            bot.send_message(m.from_user.id, f"✅ Admin {uid} added!")
            try:
                bot.send_message(uid, "🎉 You are now an admin!")
            except:
                pass
        else:
            if uid == ADMIN_ID:
                bot.send_message(m.from_user.id, "❌ Cannot remove owner!")
                return
            result = admins_col.delete_one({"_id": uid})
            if result.deleted_count > 0:
                bot.send_message(m.from_user.id, f"✅ Admin {uid} removed!")
            else:
                bot.send_message(m.from_user.id, "❌ Not an admin!")
    except:
        bot.send_message(m.from_user.id, f"❌ Use: /{cmd} user_id")

# =========================
# 📞 SET CONTACTS
# =========================
@bot.message_handler(func=lambda m: m.text == "📞 Set Contacts" and is_admin(m.from_user.id))
def set_contacts_menu(m):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("💰 Points Contact", callback_data="set_points"), InlineKeyboardButton("⭐ VIP Contact", callback_data="set_vip"), InlineKeyboardButton("📋 View", callback_data="view_contacts"))
    bot.send_message(m.from_user.id, "📞 **Contacts**", reply_markup=kb)

@bot.callback_query_handler(func=lambda c: c.data == "set_points")
def set_points_contact(c):
    msg = bot.send_message(c.from_user.id, "💰 Send @username or link:\nSend 'none' to remove", parse_mode="Markdown")
    bot.register_next_step_handler(msg, save_points_contact)
    bot.answer_callback_query(c.id)

def save_points_contact(m):
    if m.text.lower() == "none":
        set_config("contact_username", None)
        set_config("contact_link", None)
    elif m.text.startswith("http"):
        set_config("contact_link", m.text)
        set_config("contact_username", None)
    elif m.text.startswith("@"):
        set_config("contact_username", m.text)
        set_config("contact_link", None)
    else:
        bot.send_message(m.from_user.id, "❌ Invalid!")
        return
    bot.send_message(m.from_user.id, "✅ Updated!", reply_markup=admin_menu())

@bot.callback_query_handler(func=lambda c: c.data == "set_vip")
def set_vip_contact(c):
    msg = bot.send_message(c.from_user.id, "⭐ Send @username or link:\nSend 'none' to remove", parse_mode="Markdown")
    bot.register_next_step_handler(msg, save_vip_contact)
    bot.answer_callback_query(c.id)

def save_vip_contact(m):
    if m.text.lower() == "none":
        set_config("vip_contact", None)
    elif m.text.startswith("http") or m.text.startswith("@"):
        set_config("vip_contact", m.text)
    else:
        bot.send_message(m.from_user.id, "❌ Invalid!")
        return
    bot.send_message(m.from_user.id, "✅ Updated!", reply_markup=admin_menu())

@bot.callback_query_handler(func=lambda c: c.data == "view_contacts")
def view_contacts_cb(c):
    cfg = get_config()
    points = cfg.get("contact_username") or cfg.get("contact_link") or "Not set"
    vip = cfg.get("vip_contact") or "Not set"
    bot.edit_message_text(f"📞 Points: {points}\n⭐ VIP: {vip}", c.from_user.id, c.message.message_id, parse_mode="Markdown")
    bot.answer_callback_query(c.id)

# =========================
# 🔘 BUTTON MANAGER (BUTTON-BASED)
# =========================
_button_wizard = {}

def button_manager_keyboard():
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("➕ Add Link Button", callback_data="btnmgr|add|link"),
        InlineKeyboardButton("📁 Add Folder Button", callback_data="btnmgr|add|folder"),
        InlineKeyboardButton("➖ Remove Button", callback_data="btnmgr|remove"),
        InlineKeyboardButton("📋 View Buttons", callback_data="btnmgr|view"),
    )
    kb.add(InlineKeyboardButton("❌ Close", callback_data="btnmgr|close"))
    return kb

@bot.message_handler(func=lambda m: m.text == "🔘 Button Manager" and is_admin(m.from_user.id))
def button_manager_cmd(m):
    bot.send_message(m.from_user.id, "🔘 **Button Manager**\n\nChoose an action:", reply_markup=button_manager_keyboard(), parse_mode="Markdown")

@bot.callback_query_handler(func=lambda c: c.data.startswith("btnmgr|"))
def button_manager_callback(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admin only", True)
    parts = c.data.split("|")
    action = parts[1]
    try:
        if action == "close":
            bot.delete_message(c.from_user.id, c.message.message_id)
            return bot.answer_callback_query(c.id)
        if action == "view":
            buttons = get_custom_buttons()
            text = "🔘 **Custom Buttons**\n\n" + ("\n".join(f"{i+1}. {b.get('text')} — {b.get('type')}" for i,b in enumerate(buttons)) if buttons else "No custom buttons.")
            bot.send_message(c.from_user.id, text, parse_mode="Markdown")
            return bot.answer_callback_query(c.id, "List opened")
        if action == "remove":
            buttons = get_custom_buttons()
            if not buttons:
                return bot.answer_callback_query(c.id, "No custom buttons", True)
            kb = InlineKeyboardMarkup(row_width=1)
            for i,b in enumerate(buttons):
                kb.add(InlineKeyboardButton(f"❌ {b.get('text')}", callback_data=f"btnmgr|delete|{i}"))
            bot.send_message(c.from_user.id, "Select a button to remove:", reply_markup=kb)
            return bot.answer_callback_query(c.id)
        if action == "delete":
            idx = int(parts[2]); buttons = get_custom_buttons()
            if idx < 0 or idx >= len(buttons): raise ValueError("Button no longer exists")
            removed = buttons.pop(idx); set_config("custom_buttons", buttons)
            bot.edit_message_text(f"✅ Process Complete\nRemoved: {removed.get('text')}", c.from_user.id, c.message.message_id)
            return bot.answer_callback_query(c.id, "Removed")
        if action == "add":
            typ = parts[2]
            _button_wizard[c.from_user.id] = {"type": typ}
            msg = bot.send_message(c.from_user.id, "Send the button name/text:")
            bot.register_next_step_handler(msg, button_name_step)
            return bot.answer_callback_query(c.id, "Continue in chat")
    except Exception as exc:
        bot.answer_callback_query(c.id, f"Error: {exc}", True)
        bot.send_message(c.from_user.id, f"❌ Process Failed\n{exc}", reply_markup=admin_menu())

def button_name_step(m):
    try:
        state = _button_wizard.get(m.from_user.id)
        if not state: raise ValueError("Session expired")
        text = (m.text or "").strip()
        if not text or len(text) > 50: raise ValueError("Button name must be 1-50 characters")
        state["text"] = text
        prompt = "Send link, @username, username, or t.me link:" if state["type"] == "link" else "Send the folder number:"
        msg = bot.send_message(m.from_user.id, prompt)
        bot.register_next_step_handler(msg, button_data_step)
    except Exception as exc:
        bot.send_message(m.from_user.id, f"❌ Process Failed\n{exc}", reply_markup=admin_menu())

def button_data_step(m):
    try:
        state = _button_wizard.pop(m.from_user.id, None)
        if not state: raise ValueError("Session expired")
        data = (m.text or "").strip()
        if state["type"] == "link":
            data = normalize_url_or_username(data)
        else:
            if not data.isdigit() or not fs.get_by_number(int(data)): raise ValueError("Folder number not found")
        add_custom_button(state["text"], state["type"], data)
        raw_bot.send_message(m.from_user.id, f"✅ Process Complete\nButton added: {state['text']}", reply_markup=admin_menu())
    except Exception as exc:
        bot.send_message(m.from_user.id, f"❌ Process Failed\n{exc}", reply_markup=admin_menu())

# =========================
# 📢 FORCE JOIN: CHANNELS + GROUPS
# =========================
def force_join_menu_keyboard():
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("➕ Add Channel", callback_data="force|add|channel"),
        InlineKeyboardButton("➕ Add Group", callback_data="force|add|group"),
        InlineKeyboardButton("➖ Remove Channel", callback_data="force|remove|channel"),
        InlineKeyboardButton("➖ Remove Group", callback_data="force|remove|group"),
        InlineKeyboardButton("📋 View Required Chats", callback_data="force|view"),
    )
    kb.add(InlineKeyboardButton("❌ Close", callback_data="force|close"))
    return kb

@bot.message_handler(func=lambda m: m.text == "📢 Force Join" and is_admin(m.from_user.id))
def force_join_menu(m):
    bot.send_message(m.from_user.id, "📢 **Force Join Manager**\n\nFor private groups, use the numeric chat ID (`-100...`). The bot must be an admin.", reply_markup=force_join_menu_keyboard(), parse_mode="Markdown")

@bot.callback_query_handler(func=lambda c: c.data.startswith("force|"))
def force_join_callback(c):
    if not is_admin(c.from_user.id): return bot.answer_callback_query(c.id, "Admin only", True)
    _, action, *rest = c.data.split("|")
    try:
        if action == "close":
            bot.delete_message(c.from_user.id, c.message.message_id); return bot.answer_callback_query(c.id)
        if action == "view":
            cfg=get_config(); channels=cfg.get("force_channels",[]); groups=cfg.get("force_groups",[])
            text="📢 **Required Channels**\n"+("\n".join(channels) if channels else "None")+"\n\n👥 **Required Groups**\n"+("\n".join(map(str,groups)) if groups else "None")
            bot.send_message(c.from_user.id,text,parse_mode="Markdown"); return bot.answer_callback_query(c.id,"Opened")
        typ=rest[0]
        key="force_channels" if typ=="channel" else "force_groups"
        if action=="add":
            _button_wizard[c.from_user.id]={"force_key":key,"force_type":typ}
            msg=bot.send_message(c.from_user.id, "Send @username or numeric chat ID (`-100...`):", parse_mode="Markdown")
            bot.register_next_step_handler(msg, force_add_step); return bot.answer_callback_query(c.id,"Continue in chat")
        if action=="remove":
            items=get_config().get(key,[])
            if not items:return bot.answer_callback_query(c.id,"Nothing to remove",True)
            kb=InlineKeyboardMarkup(row_width=1)
            for i,item in enumerate(items):kb.add(InlineKeyboardButton(f"❌ {item}",callback_data=f"force|delete|{typ}|{i}"))
            bot.send_message(c.from_user.id,"Select an item to remove:",reply_markup=kb); return bot.answer_callback_query(c.id)
        if action=="delete":
            typ,index=rest[0],int(rest[1]); key="force_channels" if typ=="channel" else "force_groups"; items=get_config().get(key,[])
            if index<0 or index>=len(items):raise ValueError("Item no longer exists")
            removed=items.pop(index);set_config(key,items);bot.edit_message_text(f"✅ Process Complete\nRemoved: {removed}",c.from_user.id,c.message.message_id);return bot.answer_callback_query(c.id,"Removed")
    except Exception as exc:
        bot.answer_callback_query(c.id,f"Error: {exc}",True);bot.send_message(c.from_user.id,f"❌ Process Failed\n{exc}",reply_markup=admin_menu())

def force_add_step(m):
    try:
        state=_button_wizard.pop(m.from_user.id,None)
        if not state or "force_key" not in state:raise ValueError("Session expired")
        value=(m.text or "").strip()
        if not value.startswith("@"):
            try:value=str(int(value))
            except:raise ValueError("Use @username or numeric chat ID")
        # Verify bot can access the chat.
        chat=bot.get_chat(value)
        bot_member=bot.get_chat_member(chat.id,bot.get_me().id)
        if bot_member.status not in ("administrator","creator"):raise ValueError("Make the bot admin in that channel/group first")
        items=get_config().get(state["force_key"],[])
        normalized=str(chat.id) if value.lstrip("-").isdigit() else value
        if normalized in items:raise ValueError("Already added")
        items.append(normalized);set_config(state["force_key"],items)
        bot.send_message(m.from_user.id,f"✅ Process Complete\nAdded: {normalized}",reply_markup=admin_menu())
    except Exception as exc:
        bot.send_message(m.from_user.id,f"❌ Process Failed\n{exc}",reply_markup=admin_menu())

# =========================
# 👥 NEW USER JOIN NOTIFICATIONS
# =========================
def join_notification_keyboard():
    cfg=get_cached_config(); kb=InlineKeyboardMarkup(row_width=2)
    kb.add(InlineKeyboardButton("➕ Set Join Group",callback_data="joinnotify|set"),InlineKeyboardButton("🗑 Remove Join Group",callback_data="joinnotify|remove"))
    kb.add(InlineKeyboardButton(f"👤 Join Alerts: {'ON' if cfg.get('join_notify_enabled',True) else 'OFF'}",callback_data="joinnotify|togglejoin"))
    kb.add(InlineKeyboardButton("➕ Set Method Group",callback_data="joinnotify|setmethod"),InlineKeyboardButton("🗑 Remove Method Group",callback_data="joinnotify|removemethod"))
    kb.add(InlineKeyboardButton(f"🔔 Method Alerts: {'ON' if cfg.get('method_notify_enabled',True) else 'OFF'}",callback_data="joinnotify|togglemethod"))
    kb.add(InlineKeyboardButton("📋 View Settings",callback_data="joinnotify|view"))
    return kb

@bot.message_handler(func=lambda m:m.text=="👥 Join Notifications" and is_admin(m.from_user.id))
def join_notification_menu(m):
    bot.send_message(m.from_user.id,"👥 **Notification Settings**\n\nAccepts @username, username, t.me link, or numeric ID. Bot must be admin.",reply_markup=join_notification_keyboard(),parse_mode="Markdown")

@bot.callback_query_handler(func=lambda c:c.data.startswith("joinnotify|"))
def join_notification_cb(c):
    if not is_admin(c.from_user.id):return bot.answer_callback_query(c.id,"Admin only",True)
    action=c.data.split("|")[1]
    try:
        cfg=get_config()
        if action=="view":
            text=f"👤 Join group: `{cfg.get('join_notify_group') or 'Not set'}`\nJoin alerts: **{'ON' if cfg.get('join_notify_enabled',True) else 'OFF'}**\n\n🔔 Method group: `{cfg.get('method_notify_group') or cfg.get('join_notify_group') or 'Not set'}`\nMethod alerts: **{'ON' if cfg.get('method_notify_enabled',True) else 'OFF'}**"
            bot.send_message(c.from_user.id,text,parse_mode="Markdown",reply_markup=join_notification_keyboard());return bot.answer_callback_query(c.id)
        if action=="remove": set_config("join_notify_group",None);admin_success(c.from_user.id,"Join notification group removed");return bot.answer_callback_query(c.id,"Removed")
        if action=="removemethod": set_config("method_notify_group",None);admin_success(c.from_user.id,"Method notification group removed");return bot.answer_callback_query(c.id,"Removed")
        if action=="togglejoin": set_config("join_notify_enabled",not cfg.get("join_notify_enabled",True));bot.edit_message_reply_markup(c.from_user.id,c.message.message_id,reply_markup=join_notification_keyboard());return bot.answer_callback_query(c.id,"Updated")
        if action=="togglemethod": set_config("method_notify_enabled",not cfg.get("method_notify_enabled",True));bot.edit_message_reply_markup(c.from_user.id,c.message.message_id,reply_markup=join_notification_keyboard());return bot.answer_callback_query(c.id,"Updated")
        _join_notify_pending[c.from_user.id]="method" if action=="setmethod" else "join"
        msg=bot.send_message(c.from_user.id,"Send group @username, username, t.me link, or numeric ID:");bot.register_next_step_handler(msg,save_join_notification_group);bot.answer_callback_query(c.id,"Continue in chat")
    except Exception as exc: admin_error(c.from_user.id,exc)

_join_notify_pending={}
def save_join_notification_group(m):
    try:
        value=normalize_chat_reference(m.text); chat=bot.get_chat(value); member=bot.get_chat_member(chat.id,bot.get_me().id)
        if member.status not in ("administrator","creator"):raise ValueError("Bot must be admin in the group")
        kind=_join_notify_pending.pop(m.from_user.id,"join")
        key="method_notify_group" if kind=="method" else "join_notify_group"
        set_config(key,chat.id)
        if kind == "method":
            groups = get_config().get("method_notify_groups", [])
            if chat.id not in groups:
                groups.append(chat.id)
                set_config("method_notify_groups", groups)
        admin_success(m.from_user.id,f"{'Method' if kind=='method' else 'Join'} notification group set: {chat.id}")
        bot.send_message(chat.id,f"✅ This group will receive {'method upload/update' if kind=='method' else 'new-user join'} notifications.")
    except Exception as exc: admin_error(m.from_user.id,exc)

# =========================
# ⚙️ SETTINGS
# =========================
@bot.message_handler(func=lambda m: m.text == "⚙️ Settings" and is_admin(m.from_user.id))
def settings_cmd(m):
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(InlineKeyboardButton("⭐ VIP Msg", callback_data="set_vip_msg"), InlineKeyboardButton("🏠 Welcome", callback_data="set_welcome"), InlineKeyboardButton("💰 Ref Reward", callback_data="set_reward"), InlineKeyboardButton("💵 Points/$", callback_data="set_ppd"))
    bot.send_message(m.from_user.id, "⚙️ **Settings**", reply_markup=kb)

@bot.callback_query_handler(func=lambda c: c.data == "set_vip_msg")
def set_vip_msg_cb(c):
    msg = bot.send_message(c.from_user.id, "Send new VIP message:", parse_mode="Markdown")
    bot.register_next_step_handler(msg, lambda x: set_config("vip_msg", x.text) or bot.send_message(x.from_user.id, "✅ Updated!", reply_markup=admin_menu()))
    bot.answer_callback_query(c.id)

@bot.callback_query_handler(func=lambda c: c.data == "set_welcome")
def set_welcome_cb(c):
    msg = bot.send_message(c.from_user.id, "Send new welcome message:", parse_mode="Markdown")
    bot.register_next_step_handler(msg, lambda x: set_config("welcome", x.text) or bot.send_message(x.from_user.id, "✅ Updated!", reply_markup=admin_menu()))
    bot.answer_callback_query(c.id)

@bot.callback_query_handler(func=lambda c: c.data == "set_reward")
def set_reward_cb(c):
    current = get_config().get("ref_reward", 5)
    msg = bot.send_message(c.from_user.id, f"Current: {current}\nSend new amount:", parse_mode="Markdown")
    bot.register_next_step_handler(msg, lambda x: set_config("ref_reward", int(x.text)) or bot.send_message(x.from_user.id, f"✅ Set to {x.text} points!", reply_markup=admin_menu()) if x.text.isdigit() else bot.send_message(x.from_user.id, "❌ Invalid!"))
    bot.answer_callback_query(c.id)

@bot.callback_query_handler(func=lambda c: c.data == "set_ppd")
def set_ppd_cb(c):
    current = get_config().get("points_per_dollar", 100)
    msg = bot.send_message(c.from_user.id, f"Current: {current} pts = $1\nSend new value:", parse_mode="Markdown")
    bot.register_next_step_handler(msg, lambda x: set_config("points_per_dollar", int(x.text)) or bot.send_message(x.from_user.id, f"✅ Set to {x.text} pts = $1!", reply_markup=admin_menu()) if x.text.isdigit() else bot.send_message(x.from_user.id, "❌ Invalid!"))
    bot.answer_callback_query(c.id)

# =========================
# 📊 STATS (FIXED VIP COUNT)
# =========================
@bot.message_handler(func=lambda m: m.text == "📊 Stats" and is_admin(m.from_user.id))
def stats_cmd(m):
    total = users_col.count_documents({})
    vip = users_col.count_documents({"vip": True})
    free = total - vip
    
    all_u = list(users_col.find({}))
    points = sum(u.get("points", 0) for u in all_u)
    earned = sum(u.get("total_points_earned", 0) for u in all_u)
    spent = sum(u.get("total_points_spent", 0) for u in all_u)
    refs = sum(u.get("refs", 0) for u in all_u)
    purchases = sum(len(u.get("purchased_methods", [])) for u in all_u)
    
    free_f = folders_col.count_documents({"cat": "free"})
    vip_f = folders_col.count_documents({"cat": "vip"})
    apps_f = folders_col.count_documents({"cat": "apps"})
    svc_f = folders_col.count_documents({"cat": "services"})
    
    total_c, used_c, _, _ = codesys.get_stats()
    
    text = f"📊 **ZEDOX STATISTICS**\n\n"
    text += f"👥 **USERS:**\n"
    text += f"┌ Total Users: `{total}`\n"
    text += f"├ VIP Users: `{vip}`\n"
    text += f"└ Free Users: `{free}`\n\n"
    
    text += f"💰 **POINTS:**\n"
    text += f"┌ Current Total: `{points:,}`\n"
    text += f"├ Total Earned: `{earned:,}`\n"
    text += f"├ Total Spent: `{spent:,}`\n"
    text += f"└ Avg per User: `{points//total if total > 0 else 0}`\n\n"
    
    text += f"📚 **CONTENT:**\n"
    text += f"┌ FREE METHODS: `{free_f}`\n"
    text += f"├ VIP METHODS: `{vip_f}`\n"
    text += f"├ PREMIUM APPS: `{apps_f}`\n"
    text += f"└ SERVICES: `{svc_f}`\n\n"
    
    text += f"📈 **ACTIVITY:**\n"
    text += f"┌ Total Referrals: `{refs}`\n"
    text += f"├ Total Purchases: `{purchases}`\n"
    text += f"├ Total Codes: `{total_c}`\n"
    text += f"├ Used Codes: `{used_c}`\n"
    text += f"└ Unused Codes: `{total_c - used_c}`"
    
    bot.send_message(m.from_user.id, text, parse_mode="Markdown")

# =========================
# 📢 BROADCAST
# =========================
@bot.message_handler(func=lambda m: m.text == "📢 Broadcast" and is_admin(m.from_user.id))
def broadcast_cmd(m):
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(InlineKeyboardButton("All", callback_data="bc_all"), InlineKeyboardButton("VIP", callback_data="bc_vip"), InlineKeyboardButton("Free", callback_data="bc_free"))
    bot.send_message(m.from_user.id, "📢 Broadcast to:", reply_markup=kb)

@bot.callback_query_handler(func=lambda c: c.data.startswith("bc_"))
def broadcast_target_cb(c):
    target = c.data[3:]
    msg = bot.send_message(c.from_user.id, f"Send message to {target.upper()} users:", parse_mode="Markdown")
    bot.register_next_step_handler(msg, lambda x: send_broadcast(x, target))
    bot.answer_callback_query(c.id)

def send_broadcast(m, target):
    query = {}
    if target == "vip":
        query = {"vip": True}
    elif target == "free":
        query = {"vip": False}
    
    users = list(users_col.find(query))
    if not users:
        bot.send_message(m.from_user.id, "❌ No users!")
        return
    
    status = bot.send_message(m.from_user.id, f"📤 Broadcasting to {len(users)} users...")
    sent, failed = 0, 0
    
    for u in users:
        try:
            uid = int(u["_id"])
            if m.content_type == "text":
                bot.send_message(uid, m.text, parse_mode="HTML")
            elif m.content_type == "photo":
                bot.send_photo(uid, m.photo[-1].file_id, caption=m.caption, parse_mode="HTML")
            elif m.content_type == "video":
                bot.send_video(uid, m.video.file_id, caption=m.caption, parse_mode="HTML")
            elif m.content_type == "document":
                bot.send_document(uid, m.document.file_id, caption=m.caption, parse_mode="HTML")
            sent += 1
            if sent % 20 == 0:
                time.sleep(0.3)
        except:
            failed += 1
    
    bot.edit_message_text(f"✅ Done!\n📤 Sent: {sent}\n❌ Failed: {failed}", m.from_user.id, status.message_id)

# =========================
# 🔔 NOTIFY
# =========================
@bot.message_handler(commands=["legacy_notify_toggle_disabled"])
def toggle_notify_cmd(m):
    cfg=get_config(); new=not cfg.get("method_notify_enabled",True);set_config("method_notify_enabled",new)
    bot.send_message(m.from_user.id,f"🔔 Method upload/update notifications: {'ON' if new else 'OFF'}",reply_markup=admin_menu())

# =========================
# 🏦 BINANCE SETTINGS
# =========================
@bot.message_handler(func=lambda m: m.text == "🏦 Binance Settings" and is_admin(m.from_user.id))
def binance_settings_menu(m):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("💰 Coin", callback_data="set_binance_coin"), InlineKeyboardButton("🌐 Network", callback_data="set_binance_network"), InlineKeyboardButton("📍 Address", callback_data="set_binance_address"), InlineKeyboardButton("📝 Memo", callback_data="set_binance_memo"), InlineKeyboardButton("📋 View", callback_data="view_binance_settings"))
    bot.send_message(m.from_user.id, "🏦 **Binance**", reply_markup=kb)

@bot.callback_query_handler(func=lambda c: c.data == "set_binance_coin")
def set_binance_coin_cb(c):
    msg = bot.send_message(c.from_user.id, f"Coin (USDT, BUSD, BTC):\nCurrent: {get_config().get('binance_coin', 'USDT')}", parse_mode="Markdown")
    bot.register_next_step_handler(msg, lambda x: set_config("binance_coin", x.text.upper()) or bot.send_message(x.from_user.id, f"✅ Set to {x.text.upper()}", reply_markup=admin_menu()))
    bot.answer_callback_query(c.id)

@bot.callback_query_handler(func=lambda c: c.data == "set_binance_network")
def set_binance_network_cb(c):
    msg = bot.send_message(c.from_user.id, f"Network (TRC20, BEP20, ERC20):\nCurrent: {get_config().get('binance_network', 'TRC20')}", parse_mode="Markdown")
    bot.register_next_step_handler(msg, lambda x: set_config("binance_network", x.text.upper()) or bot.send_message(x.from_user.id, f"✅ Set to {x.text.upper()}", reply_markup=admin_menu()))
    bot.answer_callback_query(c.id)

@bot.callback_query_handler(func=lambda c: c.data == "set_binance_address")
def set_binance_address_cb(c):
    msg = bot.send_message(c.from_user.id, f"Address:\nCurrent: {get_config().get('binance_address', 'Not set')}", parse_mode="Markdown")
    bot.register_next_step_handler(msg, lambda x: set_config("binance_address", x.text) or bot.send_message(x.from_user.id, f"✅ Address saved!", reply_markup=admin_menu()))
    bot.answer_callback_query(c.id)

@bot.callback_query_handler(func=lambda c: c.data == "set_binance_memo")
def set_binance_memo_cb(c):
    msg = bot.send_message(c.from_user.id, f"Memo/Tag (send 'none' to clear):\nCurrent: {get_config().get('binance_memo', 'None')}", parse_mode="Markdown")
    bot.register_next_step_handler(msg, lambda x: set_config("binance_memo", "" if x.text.lower() == "none" else x.text) or bot.send_message(x.from_user.id, f"✅ Memo saved!", reply_markup=admin_menu()))
    bot.answer_callback_query(c.id)

@bot.callback_query_handler(func=lambda c: c.data == "view_binance_settings")
def view_binance_settings_cb(c):
    cfg = get_config()
    text = f"🏦 **Binance**\n\n💰 Coin: {cfg.get('binance_coin', 'USDT')}\n🌐 Network: {cfg.get('binance_network', 'TRC20')}\n📍 Address: `{cfg.get('binance_address', 'Not set')}`\n📝 Memo: `{cfg.get('binance_memo', 'None') or 'None'}`\n📸 Screenshot: {'Yes' if cfg.get('require_screenshot', True) else 'No'}"
    bot.edit_message_text(text, c.from_user.id, c.message.message_id, parse_mode="Markdown")
    bot.answer_callback_query(c.id)

# =========================
# 📸 SCREENSHOT
# =========================
@bot.message_handler(func=lambda m: m.text == "📸 Screenshot" and is_admin(m.from_user.id))
def screenshot_setting_menu(m):
    cfg = get_config()
    current = cfg.get("require_screenshot", True)
    status = "✅ ENABLED" if current else "❌ DISABLED"
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("🔘 Toggle", callback_data="toggle_screenshot"))
    bot.send_message(m.from_user.id, f"📸 **Screenshot**\n\n{status}\n\nRequire screenshot for payments.", reply_markup=kb)

@bot.callback_query_handler(func=lambda c: c.data == "toggle_screenshot")
def toggle_screenshot_cb(c):
    cfg = get_config()
    current = cfg.get("require_screenshot", True)
    set_config("require_screenshot", not current)
    new_status = "ENABLED" if not current else "DISABLED"
    bot.answer_callback_query(c.id, f"Screenshot {new_status}!")
    bot.edit_message_text(f"✅ Screenshot {new_status}!", c.from_user.id, c.message.message_id)
    bot.send_message(c.from_user.id, "Returning...", reply_markup=admin_menu())

# =========================
# 💳 PAYMENT METHODS
# =========================
@bot.message_handler(func=lambda m: m.text == "💳 Payment Methods" and is_admin(m.from_user.id))
def payment_methods_menu(m):
    methods = get_config().get("payment_methods", ["💳 Binance", "💵 USDT"])
    text = "💳 **Payment Methods**\n\n"
    for i, mtd in enumerate(methods, 1):
        text += f"{i}. {mtd}\n"
    text += "\n/addmethod name\n/removemethod number\n/listmethods"
    bot.send_message(m.from_user.id, text, parse_mode="Markdown")

@bot.message_handler(commands=["addmethod", "removemethod", "listmethods"])
def payment_commands(m):
    if not is_admin(m.from_user.id):
        return
    
    cmd = m.text.split()[0][1:]
    methods = get_config().get("payment_methods", ["💳 Binance", "💵 USDT"])
    
    if cmd == "listmethods":
        text = "💳 **Methods**\n\n"
        for i, mtd in enumerate(methods, 1):
            text += f"{i}. {mtd}\n"
        bot.send_message(m.from_user.id, text, parse_mode="Markdown")
        return
    
    try:
        if cmd == "addmethod":
            method = m.text.replace("/addmethod", "").strip()
            if not method:
                bot.send_message(m.from_user.id, "❌ Usage: /addmethod name")
                return
            methods.append(method)
            set_config("payment_methods", methods)
            bot.send_message(m.from_user.id, f"✅ Added: {method}")
        elif cmd == "removemethod":
            _, num = m.text.split()
            num = int(num) - 1
            if 0 <= num < len(methods):
                removed = methods.pop(num)
                set_config("payment_methods", methods)
                bot.send_message(m.from_user.id, f"✅ Removed: {removed}")
            else:
                bot.send_message(m.from_user.id, "❌ Invalid number!")
    except:
        bot.send_message(m.from_user.id, f"❌ Use: /{cmd} ...")

# =========================
# ⚙️ VIP SETTINGS
# =========================
@bot.message_handler(func=lambda m: m.text == "⚙️ VIP Settings" and is_admin(m.from_user.id))
def vip_settings_menu(m):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("💰 USD Price", callback_data="set_vip_price_usd"), InlineKeyboardButton("💎 Points Price", callback_data="set_vip_price_points"), InlineKeyboardButton("👥 Referral VIP", callback_data="set_ref_vip_count"), InlineKeyboardButton("🛒 Purchase VIP", callback_data="set_ref_purchase_count"), InlineKeyboardButton("📅 Duration", callback_data="set_vip_duration"), InlineKeyboardButton("📋 View", callback_data="view_vip_settings"))
    bot.send_message(m.from_user.id, "⚙️ **VIP Settings**", reply_markup=kb)

@bot.callback_query_handler(func=lambda c: c.data == "set_vip_price_usd")
def set_vip_price_usd_cb(c):
    msg = bot.send_message(c.from_user.id, f"USD Price:\nCurrent: ${get_config().get('vip_price', 50)}", parse_mode="Markdown")
    bot.register_next_step_handler(msg, lambda x: set_config("vip_price", int(x.text)) or bot.send_message(x.from_user.id, f"✅ Set to ${x.text}", reply_markup=admin_menu()) if x.text.isdigit() else bot.send_message(x.from_user.id, "❌ Invalid!"))
    bot.answer_callback_query(c.id)

@bot.callback_query_handler(func=lambda c: c.data == "set_vip_price_points")
def set_vip_price_points_cb(c):
    msg = bot.send_message(c.from_user.id, f"Points Price:\nCurrent: {get_config().get('vip_points_price', 5000)}", parse_mode="Markdown")
    bot.register_next_step_handler(msg, lambda x: set_config("vip_points_price", int(x.text)) or bot.send_message(x.from_user.id, f"✅ Set to {x.text} points", reply_markup=admin_menu()) if x.text.isdigit() else bot.send_message(x.from_user.id, "❌ Invalid!"))
    bot.answer_callback_query(c.id)

@bot.callback_query_handler(func=lambda c: c.data == "set_ref_vip_count")
def set_ref_vip_count_cb(c):
    msg = bot.send_message(c.from_user.id, f"Referrals for VIP:\nCurrent: {get_config().get('referral_vip_count', 50)}", parse_mode="Markdown")
    bot.register_next_step_handler(msg, lambda x: set_config("referral_vip_count", int(x.text)) or bot.send_message(x.from_user.id, f"✅ Set to {x.text} referrals", reply_markup=admin_menu()) if x.text.isdigit() else bot.send_message(x.from_user.id, "❌ Invalid!"))
    bot.answer_callback_query(c.id)

@bot.callback_query_handler(func=lambda c: c.data == "set_ref_purchase_count")
def set_ref_purchase_count_cb(c):
    msg = bot.send_message(c.from_user.id, f"Referral Purchases for VIP:\nCurrent: {get_config().get('referral_purchase_count', 10)}", parse_mode="Markdown")
    bot.register_next_step_handler(msg, lambda x: set_config("referral_purchase_count", int(x.text)) or bot.send_message(x.from_user.id, f"✅ Set to {x.text} purchases", reply_markup=admin_menu()) if x.text.isdigit() else bot.send_message(x.from_user.id, "❌ Invalid!"))
    bot.answer_callback_query(c.id)

@bot.callback_query_handler(func=lambda c: c.data == "set_vip_duration")
def set_vip_duration_cb(c):
    msg = bot.send_message(c.from_user.id, f"VIP Duration (days, 0 = permanent):\nCurrent: {get_config().get('vip_duration_days', 30)}", parse_mode="Markdown")
    bot.register_next_step_handler(msg, lambda x: set_config("vip_duration_days", int(x.text)) or bot.send_message(x.from_user.id, f"✅ Set to {x.text} days" + (" (permanent)" if int(x.text) == 0 else ""), reply_markup=admin_menu()) if x.text.isdigit() else bot.send_message(x.from_user.id, "❌ Invalid!"))
    bot.answer_callback_query(c.id)

@bot.callback_query_handler(func=lambda c: c.data == "view_vip_settings")
def view_vip_settings_cb(c):
    cfg = get_config()
    text = f"📋 **VIP Settings**\n\n💰 USD: ${cfg.get('vip_price', 50)}\n💎 Points: {cfg.get('vip_points_price', 5000)}\n👥 Referrals: {cfg.get('referral_vip_count', 50)}\n🛒 Purchases: {cfg.get('referral_purchase_count', 10)}\n📅 Duration: {cfg.get('vip_duration_days', 30)} days" + (" (permanent)" if cfg.get('vip_duration_days', 30) == 0 else "")
    bot.edit_message_text(text, c.from_user.id, c.message.message_id, parse_mode="Markdown")
    bot.answer_callback_query(c.id)

# =========================
# 🔗 ADD CUSTOM LINK
# =========================
@bot.message_handler(func=lambda m: m.text == "🔗 Add Custom Link" and is_admin(m.from_user.id))
def add_custom_link_cmd(m):
    msg = bot.send_message(m.from_user.id, "🔗 **Add Link**\n\nSend: `text|url`\nExample: `Website|https://example.com`", parse_mode="Markdown")
    bot.register_next_step_handler(msg, add_custom_link_process)

def add_custom_link_process(m):
    try:
        parts = m.text.split("|")
        if len(parts) != 2:
            bot.send_message(m.from_user.id, "❌ Use: text|url")
            return
        text, url = parts[0].strip(), parts[1].strip()
        if not url.startswith(("http://", "https://")):
            url = "https://" + url
        add_custom_button(text, "link", url)
        bot.send_message(m.from_user.id, f"✅ Added: {text}", reply_markup=admin_menu())
    except:
        bot.send_message(m.from_user.id, "❌ Invalid format!")

# =========================
# 📋 VIEW LINKS
# =========================
@bot.message_handler(func=lambda m: m.text == "📋 View Links" and is_admin(m.from_user.id))
def view_links_cmd(m):
    btns = get_custom_buttons()
    if not btns:
        bot.send_message(m.from_user.id, "📋 No buttons!")
        return
    text = "📋 **Buttons**\n\n"
    for i, b in enumerate(btns, 1):
        text += f"{i}. {b['text']} ({b['type']})\n"
    bot.send_message(m.from_user.id, text, parse_mode="Markdown")


# =========================
# 🧩 COMPLETE UPDATE EXTENSIONS
# =========================
TZ_OFFSET_SECONDS = 5 * 3600  # Asia/Karachi
_scheduler_stop = threading.Event()

def now_ts():
    return time.time()

def log_event(action, actor=None, target=None, details=None, level="info"):
    try:
        logs_col.insert_one({"action": action, "actor": str(actor) if actor is not None else None,
                             "target": str(target) if target is not None else None,
                             "details": details or {}, "level": level, "created_at": now_ts()})
    except Exception:
        pass

def done(uid, extra=""):
    bot.send_message(uid, "✅ Done Successfully" + (f"\n{extra}" if extra else ""), reply_markup=admin_menu())

def safe_admin(handler):
    @wraps(handler)
    def wrapped(m, *a, **kw):
        if not is_admin(m.from_user.id): return
        try: return handler(m, *a, **kw)
        except Exception as exc:
            log_event("admin_error", m.from_user.id, details={"error": str(exc), "trace": traceback.format_exc()}, level="error")
            bot.send_message(m.from_user.id, f"❌ {type(exc).__name__}: {exc}")
    return wrapped

def add_point_history(uid, amount, reason, admin_id=None, note=None):
    point_history_col.insert_one({"user_id": str(uid), "amount": int(amount), "reason": reason,
                                  "admin_id": str(admin_id) if admin_id else None, "note": note,
                                  "created_at": now_ts()})
    log_event("points_adjusted", admin_id, uid, {"amount": amount, "reason": reason, "note": note})

def atomic_adjust_points(uid, amount, reason="manual", admin_id=None, note=None):
    uid = str(uid)
    if amount < 0:
        doc = users_col.find_one_and_update({"_id": uid, "points": {"$gte": abs(amount)}},
            {"$inc": {"points": amount, "total_points_spent": abs(amount)}, "$set": {"last_active": now_ts()}},
            return_document=ReturnDocument.AFTER)
    else:
        doc = users_col.find_one_and_update({"_id": uid},
            {"$inc": {"points": amount, "total_points_earned": amount}, "$set": {"last_active": now_ts()}},
            return_document=ReturnDocument.AFTER)
    if doc:
        User._cache.pop(uid, None); User._cache_time.pop(uid, None)
        add_point_history(uid, amount, reason, admin_id, note)
    return doc



@bot.message_handler(func=lambda m: m.text == "💾 Backup/Export" and is_admin(m.from_user.id))
def backup_export_menu(m):
    bot.send_message(m.from_user.id,"💾 **Backup / Export**\n\n`/backup`\n`/export users`\n`/export vip`\n`/export referrals`\n`/export purchases`\n`/export payments`",parse_mode="Markdown")

def send_json_document(uid, name, data):
    raw=json.dumps(data,default=str,ensure_ascii=False,indent=2).encode(); f=io.BytesIO(raw); f.name=name; bot.send_document(uid,f)

@bot.message_handler(commands=["backup","export"])
def backup_export_commands(m):
    if not is_admin(m.from_user.id): return
    try:
        if m.text.startswith('/backup'):
            payload={n:list(db[n].find({})) for n in db.list_collection_names()}; raw=json.dumps(payload,default=str,ensure_ascii=False).encode()
            z=io.BytesIO();
            with zipfile.ZipFile(z,'w',zipfile.ZIP_DEFLATED) as zz: zz.writestr('zedox_backup.json',raw)
            z.seek(0); z.name=f"zedox_backup_{datetime.now().strftime('%Y%m%d_%H%M')}.zip"; bot.send_document(m.from_user.id,z); log_event('backup',m.from_user.id)
        else:
            kind=(m.text.split(maxsplit=1)[1] if len(m.text.split(maxsplit=1))>1 else 'users').lower()
            mapping={'users':(users_col,{}),'vip':(users_col,{'vip':True}),'referrals':(users_col,{'ref':{'$ne':None}}),'purchases':(purchases_col,{}),'payments':(payments_col,{})}
            col,q=mapping.get(kind,mapping['users']); send_json_document(m.from_user.id,f'{kind}.json',list(col.find(q)))
    except Exception as exc: bot.send_message(m.from_user.id,f"❌ Export failed: {exc}")



@bot.message_handler(func=lambda m: m.text == "📣 Auto Posts" and is_admin(m.from_user.id))
def auto_posts_menu(m):
    bot.send_message(m.from_user.id,"📣 **Auto Posts Manager**\n\nChoose an action:",reply_markup=auto_posts_keyboard(),parse_mode="Markdown")

def auto_posts_keyboard():
    kb=InlineKeyboardMarkup(row_width=2)
    kb.add(InlineKeyboardButton("⏱ Every Hours",callback_data="autoui|create|hours"),InlineKeyboardButton("🗓 Daily Time",callback_data="autoui|create|daily"),InlineKeyboardButton("📋 List Posts",callback_data="autoui|list"),InlineKeyboardButton("⏸ Pause",callback_data="autoui|manage|pause"),InlineKeyboardButton("▶️ Resume",callback_data="autoui|manage|resume"),InlineKeyboardButton("🗑 Delete",callback_data="autoui|manage|delete"))
    return kb

_pending_auto={}

def auto_post_item_keyboard(action):
    rows=list(auto_posts_col.find({}).sort("created_at",-1).limit(30));kb=InlineKeyboardMarkup(row_width=1)
    for x in rows:
        label=f"{len(x.get('channels') or [x.get('channel')])} chat(s) | {x.get('schedule')} {x.get('value')} | {'ON' if x.get('active') else 'OFF'}"
        kb.add(InlineKeyboardButton(label,callback_data=f"autoui|do|{action}|{x['_id']}"))
    return kb

@bot.callback_query_handler(func=lambda c:c.data.startswith("autoui|"))
def auto_ui_callback(c):
    if not is_admin(c.from_user.id):return bot.answer_callback_query(c.id,"Admin only",True)
    parts=c.data.split("|");action=parts[1]
    try:
        if action=="list":
            rows=list(auto_posts_col.find({}).sort("created_at",-1).limit(30));text="📋 **Auto Posts**\n\n"+("\n".join(f"`{x['_id']}`\n{x.get('channel')} — {x.get('schedule')} {x.get('value')} — {'ON' if x.get('active') else 'OFF'}" for x in rows) if rows else "No auto posts.")
            bot.send_message(c.from_user.id,text,parse_mode="Markdown");return bot.answer_callback_query(c.id)
        if action=="manage":
            manage=parts[2];kb=auto_post_item_keyboard(manage)
            if not kb.keyboard:return bot.answer_callback_query(c.id,"No auto posts",True)
            bot.send_message(c.from_user.id,f"Select auto post to {manage}:",reply_markup=kb);return bot.answer_callback_query(c.id)
        if action=="do":
            from bson import ObjectId
            manage,oid=parts[2],ObjectId(parts[3])
            if manage in ("pause","resume"):
                result=auto_posts_col.update_one({"_id":oid},{"$set":{"active":manage=="resume"}})
            else:result=auto_posts_col.delete_one({"_id":oid})
            if not result.modified_count and not getattr(result,"deleted_count",0):raise ValueError("Auto post not found or unchanged")
            bot.edit_message_text(f"✅ Process Complete\nAuto post {manage}d.",c.from_user.id,c.message.message_id);return bot.answer_callback_query(c.id,"Done")
        if action=="create":
            mode=parts[2];_pending_auto[c.from_user.id]={"schedule":"every_hours" if mode=="hours" else "daily"}
            msg=bot.send_message(c.from_user.id,"Send one or multiple target channels/groups. Separate them with commas or new lines.\n\nExamples:\n`@channel1, @channel2`\n`-1001234567890`",parse_mode="Markdown");bot.register_next_step_handler(msg,auto_target_step);return bot.answer_callback_query(c.id,"Continue in chat")
    except Exception as exc:bot.answer_callback_query(c.id,f"Error: {exc}",True);bot.send_message(c.from_user.id,f"❌ Process Failed\n{exc}",reply_markup=admin_menu())

def auto_target_step(m):
    try:
        state = _pending_auto.get(m.from_user.id)
        if not state:
            raise ValueError("Session expired. Open Auto Posts again")
        raw = (m.text or "").strip()
        refs = [x.strip() for x in re.split(r"[,\n]+", raw) if x.strip()]
        if not refs:
            raise ValueError("Send at least one channel or group")
        targets = []
        for ref in refs:
            target = normalize_chat_reference(ref)
            chat = bot.get_chat(target)
            targets.append(int(chat.id))
        state["channels"] = list(dict.fromkeys(targets))
        state["channel"] = state["channels"][0]  # backward compatibility
        prompt = "Send interval in hours, for example `2`:" if state["schedule"] == "every_hours" else "Send daily time as `HH:MM`:"
        msg = bot.send_message(m.from_user.id, prompt, parse_mode="Markdown")
        bot.register_next_step_handler(msg, auto_schedule_step)
    except Exception as exc:
        _pending_auto.pop(m.from_user.id, None)
        bot.send_message(m.from_user.id, f"❌ Process Failed\n{exc}", reply_markup=admin_menu())

def auto_schedule_step(m):
    try:
        state=_pending_auto.get(m.from_user.id);value=(m.text or "").strip()
        if state["schedule"]=="every_hours":
            if float(value)<=0:raise ValueError("Hours must be greater than 0")
        else:
            hh,mm=map(int,value.split(":"));
            if not(0<=hh<=23 and 0<=mm<=59):raise ValueError("Time must be HH:MM")
        state["value"]=value
        msg=bot.send_message(m.from_user.id,"Now send or forward the post content:")
        bot.register_next_step_handler(msg,auto_content_step)
    except Exception as exc:_pending_auto.pop(m.from_user.id,None);bot.send_message(m.from_user.id,f"❌ Process Failed\n{exc}",reply_markup=admin_menu())


def normalize_post_button_url(value):
    value = (value or "").strip()
    if not value:
        raise ValueError("Button link is empty")
    if value.startswith("@"):
        return "https://t.me/" + value[1:]
    if re.fullmatch(r"[A-Za-z0-9_]{5,}", value):
        return "https://t.me/" + value
    if value.startswith("t.me/"):
        return "https://" + value
    if value.startswith("telegram.me/"):
        return "https://" + value
    if value.startswith(("http://", "https://", "tg://")):
        return value
    raise ValueError("Use @username, username, t.me link, or full https:// link")

def auto_content_step(m):
    try:
        state = _pending_auto.get(m.from_user.id)
        if not state:
            raise ValueError("Session expired. Open Auto Posts again")
        state["payload"] = _message_payload(m)
        msg = bot.send_message(
            m.from_user.id,
            "Add a button below this post?\n\nSend: `Button Name | link-or-username`\nExample: `Join Channel | @zedoxprime1`\n\nSend `skip` for no button.",
            parse_mode="Markdown",
        )
        bot.register_next_step_handler(msg, save_auto_post)
    except Exception as exc:
        _pending_auto.pop(m.from_user.id, None)
        admin_error(m.from_user.id, exc)

def _message_payload(m):
    """Store the original Telegram message so formatting/entities are preserved.

    Copying the source message avoids Markdown parse errors in long posts and keeps
    link previews, custom entities, captions and media exactly as the admin sent them.
    Legacy fields are also stored as a fallback for older deployments/records.
    """
    payload = {
        "content_type": m.content_type,
        "source_chat": m.chat.id,
        "source_message": m.message_id,
    }
    if m.content_type == "text":
        payload["text"] = m.text or ""
    elif m.content_type == "photo":
        payload.update({"file_id": m.photo[-1].file_id, "caption": m.caption or ""})
    elif m.content_type == "video":
        payload.update({"file_id": m.video.file_id, "caption": m.caption or ""})
    elif m.content_type == "document":
        payload.update({"file_id": m.document.file_id, "caption": m.caption or ""})
    elif m.content_type == "animation":
        payload.update({"file_id": m.animation.file_id, "caption": m.caption or ""})
    return payload


def _payload_reply_markup(payload):
    button = payload.get("button") or {}
    if not button.get("text") or not button.get("url"):
        return None
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton(button["text"], url=button["url"]))
    return kb

def _send_payload(target, payload):
    """Send payload and return all Telegram message IDs created.

    New auto-posts are copied from the original admin message. This preserves all
    Telegram entities and prevents global Markdown parsing from breaking long text.
    """
    sent_ids = []
    reply_markup = _payload_reply_markup(payload)

    # Preferred path for all newly created auto-posts.
    if payload.get("source_chat") is not None and payload.get("source_message") is not None:
        copied = bot.copy_message(
            target,
            payload["source_chat"],
            payload["source_message"],
            reply_markup=reply_markup,
        )
        sent_ids.append(copied.message_id)
        return sent_ids

    # Backward-compatible fallback for old database records.
    typ = payload.get("content_type")
    if typ == "text":
        text = payload.get("text", "")
        chunks = [text[i:i + 4096] for i in range(0, len(text), 4096)] or [""]
        for index, chunk in enumerate(chunks):
            markup = reply_markup if index == len(chunks) - 1 else None
            sent_ids.append(raw_bot.send_message(target, chunk, reply_markup=markup).message_id)
    elif typ == "photo":
        sent_ids.append(raw_bot.send_photo(target, payload["file_id"], caption=payload.get("caption") or None, reply_markup=reply_markup).message_id)
    elif typ == "video":
        sent_ids.append(raw_bot.send_video(target, payload["file_id"], caption=payload.get("caption") or None, reply_markup=reply_markup).message_id)
    elif typ == "document":
        sent_ids.append(raw_bot.send_document(target, payload["file_id"], caption=payload.get("caption") or None, reply_markup=reply_markup).message_id)
    elif typ == "animation":
        sent_ids.append(raw_bot.send_animation(target, payload["file_id"], caption=payload.get("caption") or None, reply_markup=reply_markup).message_id)
    else:
        raise ValueError("Stored post content is unavailable. Recreate this auto post.")
    return sent_ids


def _delete_previous_auto_messages(target, message_ids):
    for message_id in message_ids or []:
        try:
            bot.delete_message(target, int(message_id))
        except Exception as exc:
            log_event("auto_post_old_delete_error", target=target, details={"message_id": message_id, "error": str(exc)}, level="warning")

def save_auto_post(m):
    try:
        x = _pending_auto.pop(m.from_user.id, None)
        if not x:
            raise ValueError("Session expired. Open Auto Posts and start again")
        now = now_ts()
        if x["schedule"] == "every_hours":
            next_run = now + float(x["value"]) * 3600
        else:
            hh, mm = map(int, x["value"].split(":"))
            local = datetime.utcfromtimestamp(now + TZ_OFFSET_SECONDS)
            nxt = local.replace(hour=hh, minute=mm, second=0, microsecond=0)
            if nxt <= local:
                nxt += timedelta(days=1)
            next_run = nxt.timestamp() - TZ_OFFSET_SECONDS
        button_input = (m.text or "").strip()
        payload = x.get("payload")
        if not payload:
            raise ValueError("Post content is missing. Start again")
        if button_input.lower() not in ("skip", "none", "no", "0"):
            if "|" not in button_input:
                raise ValueError("Use: Button Name | link-or-username, or send skip")
            button_text, button_value = [part.strip() for part in button_input.split("|", 1)]
            if not button_text:
                raise ValueError("Button name cannot be empty")
            payload["button"] = {"text": button_text[:64], "url": normalize_post_button_url(button_value)}
        channels = x.get("channels") or [x.get("channel")]
        last_messages = {}
        failures = []
        for target in channels:
            try:
                last_messages[str(target)] = _send_payload(target, payload)
            except Exception as exc:
                failures.append(f"{target}: {exc}")
        if len(failures) == len(channels):
            raise ValueError("Could not post to any channel: " + "; ".join(failures))
        doc = {**x, "channels": channels, "payload": payload, "next_run": next_run, "active": True, "created_at": now, "last_message_ids": last_messages}
        auto_posts_col.insert_one(doc)
        detail = f"Auto post created for {len(channels)} channel(s). Test post sent. Previous post will be deleted before every new post."
        if failures:
            detail += "\n⚠️ Failed targets: " + "; ".join(failures)
        admin_success(m.from_user.id, detail)
    except Exception as exc:
        admin_error(m.from_user.id, exc)

@bot.message_handler(func=lambda m: m.text == "📥 Auto Import" and is_admin(m.from_user.id))
def auto_import_menu(m):
    bot.send_message(m.from_user.id,"📥 **Auto Import / Upload**\n\nChoose an action:",reply_markup=auto_import_keyboard(),parse_mode="Markdown")

def auto_import_keyboard():
    kb=InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("➕ Add Source", callback_data="importui|sourceadd"),
        InlineKeyboardButton("➖ Remove Source", callback_data="importui|sourceremove"),
        InlineKeyboardButton("📋 View Sources", callback_data="importui|sourcelist"),
        InlineKeyboardButton("📤 Import/Upload Method", callback_data="importui|method"),
        InlineKeyboardButton("📚 Import Old Method", callback_data="importui|oldmethod"),
        InlineKeyboardButton("🆓 Set FREE by Link/ID", callback_data="importui|setfree"),
        InlineKeyboardButton("💎 Set VIP by Link/ID", callback_data="importui|setvip"),
        InlineKeyboardButton("🆓 Use Recent Chat as FREE", callback_data="importui|recentfree"),
        InlineKeyboardButton("💎 Use Recent Chat as VIP", callback_data="importui|recentvip"),
        InlineKeyboardButton("📋 View Auto Channels", callback_data="importui|viewauto"),
    )
    return kb

_import_state={}
@bot.callback_query_handler(func=lambda c:c.data.startswith("importui|"))
def import_ui_callback(c):
    if not is_admin(c.from_user.id):return bot.answer_callback_query(c.id,"Admin only",True)
    action=c.data.split("|")[1]
    try:
        if action=="sourcelist":
            rows=list(source_chats_col.find({}));bot.send_message(c.from_user.id,"📋 **Sources**\n\n"+("\n".join(str(x['_id']) for x in rows) if rows else "No sources."),parse_mode="Markdown");return bot.answer_callback_query(c.id)
        if action=="sourceremove":
            rows=list(source_chats_col.find({}));
            if not rows:return bot.answer_callback_query(c.id,"No sources",True)
            kb=InlineKeyboardMarkup(row_width=1)
            for i,x in enumerate(rows):kb.add(InlineKeyboardButton(f"❌ {x['_id']}",callback_data=f"importui|deletesource|{i}"))
            _import_state[c.from_user.id]={"sources":[x['_id'] for x in rows]};bot.send_message(c.from_user.id,"Select source to remove:",reply_markup=kb);return bot.answer_callback_query(c.id)
        if action=="deletesource":
            state=_import_state.get(c.from_user.id,{});idx=int(c.data.split("|")[2]);src=state.get("sources",[])[idx];source_chats_col.delete_one({"_id":src});bot.edit_message_text(f"✅ Process Complete\nRemoved source: {src}",c.from_user.id,c.message.message_id);return bot.answer_callback_query(c.id,"Removed")
        if action=="viewauto":
            cfg=get_config();bot.send_message(c.from_user.id,f"🆓 FREE source: `{cfg.get('auto_import_free_source') or 'Not set'}`\n💎 VIP source: `{cfg.get('auto_import_vip_source') or 'Not set'}`",parse_mode="Markdown");return bot.answer_callback_query(c.id)
        if action in ("recentfree", "recentvip"):
            cfg = get_config()
            chat_id = cfg.get("recent_admin_chat_id")
            title = cfg.get("recent_admin_chat_title") or str(chat_id or "")
            if not chat_id:
                raise ValueError("No recent private chat detected. Add the bot as administrator in the group/channel first, then reopen this menu.")
            category = "free" if action == "recentfree" else "vip"
            key = "auto_import_free_source" if category == "free" else "auto_import_vip_source"
            set_config(key, int(chat_id))
            source_chats_col.update_one(
                {"_id": int(chat_id)},
                {"$set": {"active": True, "category": category, "title": title, "added_at": now_ts()}},
                upsert=True,
            )
            admin_success(c.from_user.id, f"{category.upper()} private source set: {title} (`{chat_id}`)")
            return bot.answer_callback_query(c.id, "Source saved")
        if action in ("setfree","setvip"):
            _import_state[c.from_user.id]={"set_source_category":"free" if action=="setfree" else "vip"}
            msg=bot.send_message(c.from_user.id,"Send channel @username, username, t.me link, or numeric ID. Make bot admin:");bot.register_next_step_handler(msg,import_set_category_source);return bot.answer_callback_query(c.id,"Continue in chat")
        if action=="sourceadd":
            msg=bot.send_message(c.from_user.id,"Send source chat @username, username, t.me link, or numeric ID:");bot.register_next_step_handler(msg,import_add_source_step);return bot.answer_callback_query(c.id,"Continue in chat")
        if action in ("method", "oldmethod"):
            _import_state[c.from_user.id]={"step":"category", "old_import": action == "oldmethod"};kb=InlineKeyboardMarkup(row_width=2)
            for cat,label in [("free","FREE"),("vip","VIP"),("apps","APPS"),("services","SERVICES")]:kb.add(InlineKeyboardButton(label,callback_data=f"importcat|{cat}"))
            bot.send_message(c.from_user.id,"Choose destination category:",reply_markup=kb);return bot.answer_callback_query(c.id)
    except Exception as exc:bot.send_message(c.from_user.id,f"❌ Process Failed\n{exc}",reply_markup=admin_menu())

def import_add_source_step(m):
    try:
        src=normalize_chat_reference(m.text);bot.get_chat(src);source_chats_col.update_one({'_id':src},{'$set':{'active':True,'added_at':now_ts()}},upsert=True);bot.send_message(m.from_user.id,f"✅ Process Complete\nSource added: {src}",reply_markup=admin_menu())
    except Exception as exc:bot.send_message(m.from_user.id,f"❌ Process Failed\n{exc}",reply_markup=admin_menu())

def import_set_category_source(m):
    try:
        st=_import_state.pop(m.from_user.id,None)
        if not st: raise ValueError("Session expired")
        ref=normalize_chat_reference(m.text);chat=bot.get_chat(ref);member=bot.get_chat_member(chat.id,bot.get_me().id)
        if member.status not in ("administrator","creator"): raise ValueError("Make the bot admin in that channel")
        key="auto_import_free_source" if st["set_source_category"]=="free" else "auto_import_vip_source"
        set_config(key,chat.id);source_chats_col.update_one({"_id":chat.id},{"$set":{"active":True,"category":st["set_source_category"],"added_at":now_ts()}},upsert=True)
        admin_success(m.from_user.id,f"{st['set_source_category'].upper()} auto-import channel set: {chat.id}")
    except Exception as exc: admin_error(m.from_user.id,exc)

@bot.callback_query_handler(func=lambda c:c.data.startswith("importcat|"))
def import_category_cb(c):
    if not is_admin(c.from_user.id):return
    _import_state[c.from_user.id]={"category":c.data.split("|",1)[1]};msg=bot.send_message(c.from_user.id,"Send price in points (0 for free):");bot.register_next_step_handler(msg,import_price_step);bot.answer_callback_query(c.id)

def import_price_step(m):
    try:
        state=_import_state.get(m.from_user.id);price=int((m.text or "").strip());
        if price<0:raise ValueError("Price cannot be negative")
        state["price"]=price
        prompt = "Forward the old method post from your group/channel to me now:" if state.get("old_import") else "Now send or forward the method file/message:"
        msg=bot.send_message(m.from_user.id,prompt);bot.register_next_step_handler(msg,import_method_step)
    except Exception as exc:_import_state.pop(m.from_user.id,None);bot.send_message(m.from_user.id,f"❌ Process Failed\n{exc}",reply_markup=admin_menu())

def import_method_step(m):
    try:
        state=_import_state.pop(m.from_user.id,None)
        if not state:raise ValueError("Session expired")
        name=((m.text or m.caption or 'Imported Method').strip().splitlines()[0][:100]);files=[{'chat':m.chat.id,'msg':m.message_id,'type':m.content_type}];number=fs.add(state['category'],name,files,state['price']);send_method_notification('uploaded',fs.get_by_number(number) or {'cat':state['category'],'name':name,'number':number,'price':state['price']});log_event('method_imported',m.from_user.id,number,{'name':name});raw_bot.send_message(m.from_user.id,f"✅ Process Complete\nImported: {name}",reply_markup=admin_menu())
    except Exception as exc:bot.send_message(m.from_user.id,f"❌ Process Failed\n{exc}",reply_markup=admin_menu())

@bot.my_chat_member_handler()
def remember_admin_chat(update):
    """Remember private groups/channels when the bot is promoted to administrator."""
    try:
        chat = update.chat
        new_status = update.new_chat_member.status
        if new_status not in ("administrator", "creator"):
            return
        if chat.type not in ("group", "supergroup", "channel"):
            return
        title = getattr(chat, "title", None) or str(chat.id)
        set_config("recent_admin_chat_id", int(chat.id))
        set_config("recent_admin_chat_title", title)
        source_chats_col.update_one(
            {"_id": int(chat.id)},
            {"$set": {"active": True, "title": title, "detected_at": now_ts(), "detected_by_admin_event": True}},
            upsert=True,
        )
        # Automatically classify obvious names; otherwise admin can select Recent Chat in Auto Import.
        upper_title = title.upper()
        category = None
        if "VIP" in upper_title or "PREMIUM" in upper_title:
            category = "vip"
        elif "FREE" in upper_title:
            category = "free"
        if category:
            key = "auto_import_vip_source" if category == "vip" else "auto_import_free_source"
            set_config(key, int(chat.id))
            source_chats_col.update_one({"_id": int(chat.id)}, {"$set": {"category": category}})
        try:
            detected = f"\n✅ Automatically selected as **{category.upper()}** source." if category else "\nOpen Auto Import and tap **Use Recent Chat as FREE/VIP**."
            bot.send_message(
                ADMIN_ID,
                f"🤖 **Private Chat Detected**\n\n📌 {title}\n🆔 `{chat.id}`\nType: {chat.type}{detected}",
                parse_mode="Markdown",
            )
        except Exception:
            pass
    except Exception as exc:
        log_event("remember_admin_chat_error", details={"error": str(exc)}, level="error")


def _set_import_source_from_chat(message, category):
    try:
        chat = message.chat
        if chat.type not in ("group", "supergroup", "channel"):
            raise ValueError("Send this command inside the source group/channel")
        member = bot.get_chat_member(chat.id, bot.get_me().id)
        if member.status not in ("administrator", "creator"):
            raise ValueError("Make the bot administrator first")
        key = "auto_import_vip_source" if category == "vip" else "auto_import_free_source"
        set_config(key, int(chat.id))
        set_config("recent_admin_chat_id", int(chat.id))
        set_config("recent_admin_chat_title", getattr(chat, "title", None) or str(chat.id))
        source_chats_col.update_one(
            {"_id": int(chat.id)},
            {"$set": {"active": True, "category": category, "title": getattr(chat, "title", None), "added_at": now_ts()}},
            upsert=True,
        )
        bot.send_message(chat.id, f"✅ This private chat is now the {category.upper()} auto-import source.")
        try:
            admin_success(ADMIN_ID, f"{category.upper()} private source connected: {getattr(chat, 'title', chat.id)} (`{chat.id}`)")
        except Exception:
            pass
    except Exception as exc:
        try:
            bot.send_message(message.chat.id, f"❌ Process Failed\n{exc}")
        except Exception:
            pass


@bot.message_handler(commands=["setvipimport"])
def set_vip_import_here(m):
    _set_import_source_from_chat(m, "vip")


@bot.message_handler(commands=["setfreeimport"])
def set_free_import_here(m):
    _set_import_source_from_chat(m, "free")


def _auto_import_category_for_chat(chat_id):
    """Return FREE/VIP category for a configured auto-import source chat."""
    cfg = get_cached_config()

    def same_chat(saved, current):
        if saved is None:
            return False
        try:
            return int(str(saved).strip()) == int(current)
        except (TypeError, ValueError):
            return str(saved).strip().lower() == str(current).strip().lower()

    if same_chat(cfg.get("auto_import_free_source"), chat_id):
        return "free"
    if same_chat(cfg.get("auto_import_vip_source"), chat_id):
        return "vip"
    return None


def _import_payload_message(command_message, part):
    """For #methodN.part replies, import the replied content, not the tag message."""
    replied = getattr(command_message, "reply_to_message", None)
    if part and replied is not None:
        return replied
    return command_message


def _queue_auto_import(m, edited=False):
    """
    Queue new/edited source messages for approval.

    Supported:
      #method1 + method name on second line -> main method
      #method1.2 / #method1_2 / #method1-2 -> attach another item
      #add1 / #part1 / #method1add -> simpler additional-file formats
      Tags may be used as a reply or directly in a media caption.
      editing an old group/channel post and adding #method -> queue it
    """
    try:
        raw_command = (m.text or m.caption or "").strip()
        lowered = raw_command.lower()
        if lowered.startswith("/setvipimport") or lowered == "#setvipimport":
            return _set_import_source_from_chat(m, "vip")
        if lowered.startswith("/setfreeimport") or lowered == "#setfreeimport":
            return _set_import_source_from_chat(m, "free")

        cat = _auto_import_category_for_chat(m.chat.id)
        if not cat:
            return

        first = raw_command.splitlines()[0].strip() if raw_command else ""

        # Main method formats:
        #   #method9
        # Additional-file formats (dot-free alternatives are recommended):
        #   #method9.2, #method9_2, #method9-2
        #   #add9, #part9, #method9add
        main_match = re.fullmatch(r"#method(\d+)", first, re.I)
        bare_new_match = re.fullmatch(r"#method", first, re.I)
        part_match = re.fullmatch(r"#method(\d+)[._-](\d+)", first, re.I)
        simple_add_match = re.fullmatch(r"#(?:add|part)(\d+)(?:[._-]?(\d+))?", first, re.I)
        method_add_match = re.fullmatch(r"#method(\d+)add", first, re.I)

        if main_match:
            method_key = int(main_match.group(1))
            # When #methodN is sent as a reply and method N already exists,
            # treat it as an additional file. This is the simplest reliable format.
            replied = getattr(m, "reply_to_message", None)
            existing_for_reply = folders_col.find_one({
                "cat": cat,
                "$or": [{"auto_method_key": method_key}, {"number": method_key}],
            }) if replied is not None else None
            part = 1 if existing_for_reply else 0
        elif bare_new_match:
            # Reply #method to any old untagged post to queue it as a new method.
            if not getattr(m, "reply_to_message", None):
                return
            method_key = int(get_config().get("next_folder_number", 1))
            part = 0
        elif part_match:
            method_key = int(part_match.group(1))
            part = int(part_match.group(2))
        elif simple_add_match:
            method_key = int(simple_add_match.group(1))
            part = int(simple_add_match.group(2) or 1)
        elif method_add_match:
            method_key = int(method_add_match.group(1))
            part = 1
        else:
            return

        # If the tag is sent as a reply, import the replied message. If the tag
        # is written in a media caption, import that media message itself.
        payload = getattr(m, "reply_to_message", None) if bare_new_match else _import_payload_message(m, part)
        file_item = {
            "chat": payload.chat.id,
            "msg": payload.message_id,
            "type": payload.content_type,
        }
        cfg = get_cached_config()
        source_reply_enabled = cfg.get("group_import_notify_enabled", True)

        if part:
            # Global admins can append replied content directly to an approved method.
            # This avoids a second approval step for files deliberately added by admin.
            approved_direct = folders_col.find_one({
                "cat": cat,
                "$or": [{"auto_method_key": method_key}, {"number": method_key}],
            })
            sender_id = getattr(getattr(m, "from_user", None), "id", None)
            sender_is_source_admin = False
            try:
                if sender_id:
                    member = bot.get_chat_member(m.chat.id, sender_id)
                    sender_is_source_admin = member.status in ("administrator", "creator")
            except Exception:
                sender_is_source_admin = bool(sender_id and is_admin(sender_id))
            if approved_direct and (sender_is_source_admin or (sender_id and is_admin(sender_id))):
                folders_col.update_one(
                    {"_id": approved_direct["_id"]},
                    {"$addToSet": {"files": file_item}, "$set": {"updated_at": now_ts()}},
                )
                if source_reply_enabled:
                    raw_bot.send_message(m.chat.id, f"✅ File added directly to {approved_direct.get('name')}.")
                send_method_notification("updated", folders_col.find_one({"_id": approved_direct["_id"]}))
                return

            pending = pending_methods_col.find_one({
                "cat": cat,
                "$or": [
                    {"auto_method_key": method_key},
                    {"number": method_key},
                ],
                "status": {"$in": ["pending", "pending_update", "pending_replace"]},
            })
            if pending:
                pending_methods_col.update_one(
                    {"_id": pending["_id"]},
                    {
                        "$addToSet": {"files": file_item},
                        "$set": {"updated_at": now_ts()},
                    },
                )
                if source_reply_enabled:
                    raw_bot.send_message(m.chat.id, f"✅ File added to method {method_key}. Waiting for admin approval.")
                return

            approved = folders_col.find_one({
                "cat": cat,
                "$or": [
                    {"auto_method_key": method_key},
                    {"number": method_key},
                ],
            })
            if approved:
                pending_methods_col.update_one(
                    {"cat": cat, "auto_method_key": method_key, "status": "pending_update"},
                    {
                        "$setOnInsert": {
                            "cat": cat,
                            "auto_method_key": method_key,
                            "name": approved.get("name"),
                            "source_chat": m.chat.id,
                            "created_at": now_ts(),
                            "status": "pending_update",
                            "existing_folder_id": approved.get("_id"),
                            "submitted_by": getattr(getattr(m, "from_user", None), "id", None),
                        },
                        "$addToSet": {"files": file_item},
                        "$set": {"updated_at": now_ts()},
                    },
                    upsert=True,
                )
                if source_reply_enabled:
                    raw_bot.send_message(m.chat.id, f"✅ File queued for admin approval for {approved.get('name')}.")
                return

            if source_reply_enabled:
                raw_bot.send_message(m.chat.id, f"❌ Method {method_key} was not found. Add/approve the main method first, then use #add{method_key}.")
            return

        lines = raw_command.splitlines()
        if len(lines) > 1 and lines[1].strip():
            name = lines[1].strip()[:150]
        else:
            # When an old post is edited, use its first meaningful line after the tag.
            payload_text = (getattr(payload, "text", None) or getattr(payload, "caption", None) or "").strip()
            payload_lines = [line.strip() for line in payload_text.splitlines() if line.strip()]
            name = ((payload_lines[0] if bare_new_match and payload_lines else (payload_lines[1] if len(payload_lines) > 1 else f"Method {method_key}")))[:150]

        existing_pending = pending_methods_col.find_one({
            "cat": cat,
            "auto_method_key": method_key,
            "status": {"$in": ["pending", "pending_update", "pending_replace"]},
        })
        existing_live = folders_col.find_one({"cat": cat, "auto_method_key": method_key})

        if existing_pending:
            pending_methods_col.update_one(
                {"_id": existing_pending["_id"]},
                {
                    "$set": {
                        "name": name,
                        "files": [file_item],
                        "updated_at": now_ts(),
                        "edited_source": bool(edited),
                    }
                },
            )
            pending_id = existing_pending["_id"]
            action = "updated in the pending queue"
        elif existing_live:
            doc = {
                "cat": cat,
                "auto_method_key": method_key,
                "name": name,
                "files": [file_item],
                "source_chat": m.chat.id,
                "created_at": now_ts(),
                "updated_at": now_ts(),
                "status": "pending_replace",
                "existing_folder_id": existing_live.get("_id"),
                "submitted_by": getattr(getattr(m, "from_user", None), "id", None),
                "edited_source": bool(edited),
            }
            pending_id = pending_methods_col.insert_one(doc).inserted_id
            action = "queued as an update"
        else:
            doc = {
                "cat": cat,
                "auto_method_key": method_key,
                "name": name,
                "files": [file_item],
                "source_chat": m.chat.id,
                "created_at": now_ts(),
                "updated_at": now_ts(),
                "status": "pending",
                "submitted_by": getattr(getattr(m, "from_user", None), "id", None),
                "edited_source": bool(edited),
            }
            pending_id = pending_methods_col.insert_one(doc).inserted_id
            action = "sent for approval"

        if source_reply_enabled:
            raw_bot.send_message(m.chat.id, f"⏳ {name} has been {action}. It is not visible until admin approval.")

        kb = InlineKeyboardMarkup(row_width=2)
        kb.add(
            InlineKeyboardButton("✅ Review & Approve", callback_data=f"pendingview|{pending_id}"),
            InlineKeyboardButton("❌ Reject", callback_data=f"pendingreject|{pending_id}"),
        )
        raw_bot.send_message(
            ADMIN_ID,
            f"⏳ PENDING METHOD\n\n📁 Category: {cat.upper()}\n🏷 Name: {name}\n🏷 Source: #method{method_key}\n📎 Files: 1\n✏️ Edited old post: {'Yes' if edited else 'No'}",
            reply_markup=kb,
        )
    except Exception as exc:
        try:
            raw_bot.send_message(m.chat.id, f"❌ Auto import failed: {str(exc)[:500]}")
        except Exception:
            pass
        log_event("auto_import_error", target=getattr(m.chat, "id", None), details={"error": str(exc)}, level="error")



# =========================
# 🛡 GROUP MANAGEMENT
# =========================
_vip_member_cache = {}
_vip_badge_cooldown = {}
_group_manage_pending = {}


def _managed_group_ids():
    values = get_cached_config().get("managed_chat_groups", []) or []
    result = []
    for value in values:
        try:
            result.append(int(value))
        except (TypeError, ValueError):
            continue
    return result


def _is_chat_member_status(status):
    return status in ("member", "administrator", "creator", "restricted")


def _is_vip_group_member(user_id):
    """VIP means internal VIP or membership in the configured VIP source chat."""
    try:
        if User(user_id).is_vip():
            return True
    except Exception:
        pass
    source = get_cached_config().get("auto_import_vip_source")
    if not source:
        return False
    key = (str(source), int(user_id))
    cached = _vip_member_cache.get(key)
    if cached and time.time() - cached[1] < 300:
        return cached[0]
    try:
        member = bot.get_chat_member(int(source), int(user_id))
        result = _is_chat_member_status(member.status)
    except Exception:
        result = False
    _vip_member_cache[key] = (result, time.time())
    return result


def _looks_promotional(text):
    text = (text or "").strip()
    if not text:
        return False
    patterns = [
        r"https?://\S+",
        r"(?:t\.me|telegram\.me)/\S+",
        r"(?<!\w)@[A-Za-z0-9_]{5,}",
        r"(?<!\w)[A-Za-z0-9_]{5,}bot(?!\w)",
    ]
    if any(re.search(pattern, text, re.I) for pattern in patterns):
        return True
    promo_words = r"\b(?:buy|sell|selling|available|discount|promo|promotion|offer|dm me|contact me|inbox me|price)\b"
    return bool(re.search(promo_words, text, re.I) and re.search(r"[$€£₹₨]|\d", text))


def _send_vip_badge(message):
    cfg = get_cached_config()
    if not cfg.get("group_vip_badge_enabled", True):
        return
    key = (message.chat.id, message.from_user.id)
    if time.time() - _vip_badge_cooldown.get(key, 0) < 21600:
        return
    _vip_badge_cooldown[key] = time.time()
    try:
        raw_bot.reply_to(message, "👑 VIP MEMBER", disable_notification=True)
    except Exception:
        pass


def _log_managed_group_message(message):
    try:
        if message.chat.id in _managed_group_ids():
            group_message_log_col.update_one(
                {"group_id": int(message.chat.id), "message_id": int(message.message_id)},
                {"$set": {"created_at": now_ts(), "user_id": getattr(getattr(message, "from_user", None), "id", None)}},
                upsert=True,
            )
    except Exception:
        pass


def _apply_warning_action(message, warnings):
    cfg = get_cached_config()
    limit = max(1, int(cfg.get("group_warning_limit", 3)))
    if warnings < limit:
        return None
    action = str(cfg.get("group_warning_action", "mute")).lower()
    try:
        if action == "ban":
            bot.ban_chat_member(message.chat.id, message.from_user.id)
            return "banned"
        minutes = max(1, int(cfg.get("group_mute_minutes", 1440)))
        until = datetime.utcnow() + timedelta(minutes=minutes)
        bot.restrict_chat_member(
            message.chat.id,
            message.from_user.id,
            until_date=until,
            can_send_messages=False,
            can_send_audios=False,
            can_send_documents=False,
            can_send_photos=False,
            can_send_videos=False,
            can_send_video_notes=False,
            can_send_voice_notes=False,
            can_send_polls=False,
            can_send_other_messages=False,
            can_add_web_page_previews=False,
            can_change_info=False,
            can_invite_users=False,
            can_pin_messages=False,
            can_manage_topics=False,
        )
        return f"muted for {minutes} minutes"
    except Exception as exc:
        log_event("group_warning_action_error", target=message.chat.id, details={"error": str(exc)}, level="error")
        return None


def moderate_managed_group_message(message):
    """Delete promotional content from non-VIP members and warn them."""
    try:
        _log_managed_group_message(message)
        if message.chat.id not in _managed_group_ids() or not message.from_user:
            return
        if message.from_user.is_bot or is_admin(message.from_user.id):
            return
        vip = _is_vip_group_member(message.from_user.id)
        if vip:
            _send_vip_badge(message)
            return
        cfg = get_cached_config()
        if not cfg.get("group_moderation_enabled", True):
            return
        text = message.text or message.caption or ""
        if not _looks_promotional(text):
            return
        try:
            bot.delete_message(message.chat.id, message.message_id)
        except Exception:
            pass
        record = group_warnings_col.find_one_and_update(
            {"group_id": message.chat.id, "user_id": message.from_user.id},
            {"$inc": {"warnings": 1}, "$set": {"updated_at": now_ts(), "name": message.from_user.first_name, "username": message.from_user.username}},
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        warnings = int((record or {}).get("warnings", 1))
        name = message.from_user.first_name or "Member"
        warning = (
            f"⚠️ {name}, promotional messages, links, usernames and bot mentions are not allowed for free members.\n"
            f"Warning: {warnings}\n\n👑 VIP members are allowed to promote."
        )
        action_taken = _apply_warning_action(message, warnings)
        if action_taken:
            warning += f"\n\n🚫 Limit reached: user was {action_taken}."
            group_warnings_col.update_one(
                {"group_id": message.chat.id, "user_id": message.from_user.id},
                {"$set": {"action_taken": action_taken, "action_at": now_ts()}},
            )
        raw_bot.send_message(message.chat.id, warning, disable_notification=True)
    except Exception as exc:
        log_event("group_moderation_error", target=getattr(message.chat, "id", None), details={"error": str(exc)}, level="error")


def _group_management_keyboard():
    cfg = get_cached_config()
    moderation = cfg.get("group_moderation_enabled", True)
    badge = cfg.get("group_vip_badge_enabled", True)
    user_alerts = cfg.get("user_method_notifications_enabled", True)
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(InlineKeyboardButton("➕ Add Chat Group", callback_data="groupmgr|add"), InlineKeyboardButton("➖ Remove Group", callback_data="groupmgr|remove"))
    kb.add(InlineKeyboardButton(f"🛡 Moderation: {'ON' if moderation else 'OFF'}", callback_data="groupmgr|togglemod"))
    kb.add(InlineKeyboardButton(f"👑 VIP Badge: {'ON' if badge else 'OFF'}", callback_data="groupmgr|togglebadge"))
    kb.add(InlineKeyboardButton(f"🔔 User Method Alerts: {'ON' if user_alerts else 'OFF'}", callback_data="groupmgr|toggleusers"))
    kb.add(InlineKeyboardButton(f"🎁 Referral Alerts: {'ON' if cfg.get('user_referral_notifications_enabled', True) else 'OFF'}", callback_data="groupmgr|togglerefs"))
    kb.add(InlineKeyboardButton("⚠️ Warning Limit", callback_data="groupmgr|warnlimit"), InlineKeyboardButton("🚫 Ban / Mute", callback_data="groupmgr|warnaction"))
    kb.add(InlineKeyboardButton("📜 Group Rules", callback_data="groupmgr|rules"), InlineKeyboardButton("📤 Send Rules", callback_data="groupmgr|sendrules"))
    kb.add(InlineKeyboardButton("🧹 Clear Tracked Messages", callback_data="groupmgr|clearmessages"))
    kb.add(InlineKeyboardButton("🔓 Unmute / Unban", callback_data="groupmgr|restoreuser"))
    kb.add(InlineKeyboardButton("📋 View Settings", callback_data="groupmgr|view"), InlineKeyboardButton("🧹 Clear Warnings", callback_data="groupmgr|clearwarn"))
    return kb


@bot.message_handler(func=lambda m: m.text == "🛡 Group Management" and is_admin(m.from_user.id))
def group_management_menu(m):
    raw_bot.send_message(m.from_user.id, "🛡 GROUP MANAGEMENT\n\nManage promotion protection, VIP exemptions and method alerts.", reply_markup=_group_management_keyboard())


@bot.callback_query_handler(func=lambda c: c.data.startswith("groupmgr|"))
def group_management_callback(c):
    if not is_admin(c.from_user.id):
        return
    try:
        action = c.data.split("|", 1)[1]
        cfg = get_config()
        if action == "togglemod":
            set_config("group_moderation_enabled", not cfg.get("group_moderation_enabled", True))
        elif action == "togglebadge":
            set_config("group_vip_badge_enabled", not cfg.get("group_vip_badge_enabled", True))
        elif action == "toggleusers":
            set_config("user_method_notifications_enabled", not cfg.get("user_method_notifications_enabled", True))
        elif action == "togglerefs":
            set_config("user_referral_notifications_enabled", not cfg.get("user_referral_notifications_enabled", True))
        elif action == "view":
            groups = _managed_group_ids()
            vip_source = cfg.get("auto_import_vip_source") or "Not set"
            text = "🛡 GROUP MANAGEMENT SETTINGS\n\nManaged groups:\n" + ("\n".join(map(str, groups)) if groups else "None")
            text += f"\n\nVIP membership source: {vip_source}\nModeration: {'ON' if cfg.get('group_moderation_enabled', True) else 'OFF'}\nVIP badge replies: {'ON' if cfg.get('group_vip_badge_enabled', True) else 'OFF'}\nUser method alerts: {'ON' if cfg.get('user_method_notifications_enabled', True) else 'OFF'}\nReferral alerts: {'ON' if cfg.get('user_referral_notifications_enabled', True) else 'OFF'}\nWarning limit: {cfg.get('group_warning_limit', 3)}\nAction: {cfg.get('group_warning_action', 'mute').upper()}\nMute minutes: {cfg.get('group_mute_minutes', 1440)}"
            raw_bot.send_message(c.from_user.id, text)
            return bot.answer_callback_query(c.id)
        elif action == "warnlimit":
            msg = raw_bot.send_message(c.from_user.id, "Send the number of warnings before action (example: 3).")
            bot.register_next_step_handler(msg, save_group_warning_limit)
            return bot.answer_callback_query(c.id, "Continue in chat")
        elif action == "warnaction":
            kb = InlineKeyboardMarkup(row_width=2)
            kb.add(InlineKeyboardButton("🔇 Mute", callback_data="groupwarnaction|mute"), InlineKeyboardButton("🚫 Ban", callback_data="groupwarnaction|ban"))
            raw_bot.send_message(c.from_user.id, "Choose what happens when the warning limit is reached:", reply_markup=kb)
            return bot.answer_callback_query(c.id)
        elif action == "rules":
            msg = raw_bot.send_message(c.from_user.id, "Send the full rules message. You can use multiple lines and emojis.")
            bot.register_next_step_handler(msg, save_group_rules_text)
            return bot.answer_callback_query(c.id, "Continue in chat")
        elif action == "sendrules":
            send_rules_group_picker(c.from_user.id)
            return bot.answer_callback_query(c.id)
        elif action == "clearmessages":
            send_clear_messages_group_picker(c.from_user.id)
            return bot.answer_callback_query(c.id)
        elif action == "restoreuser":
            send_restricted_users_picker(c.from_user.id)
            return bot.answer_callback_query(c.id)
        elif action == "clearwarn":
            group_warnings_col.delete_many({})
            admin_success(c.from_user.id, "All group warnings cleared")
            return bot.answer_callback_query(c.id, "Cleared")
        elif action == "add":
            _group_manage_pending[c.from_user.id] = "add"
            msg = raw_bot.send_message(c.from_user.id, "Send the chat group @username, t.me link, or numeric -100... ID. The bot must be an administrator there.")
            bot.register_next_step_handler(msg, save_managed_group)
            return bot.answer_callback_query(c.id, "Continue in chat")
        elif action == "remove":
            groups = _managed_group_ids()
            if not groups:
                raise ValueError("No managed groups are configured")
            kb = InlineKeyboardMarkup(row_width=1)
            for gid in groups:
                try:
                    title = bot.get_chat(gid).title or str(gid)
                except Exception:
                    title = str(gid)
                kb.add(InlineKeyboardButton(f"➖ {title}", callback_data=f"groupmgrremove|{gid}"))
            raw_bot.send_message(c.from_user.id, "Select a group to remove:", reply_markup=kb)
            return bot.answer_callback_query(c.id)
        bot.edit_message_reply_markup(c.from_user.id, c.message.message_id, reply_markup=_group_management_keyboard())
        bot.answer_callback_query(c.id, "Updated")
    except Exception as exc:
        bot.answer_callback_query(c.id, "Failed", True)
        admin_error(c.from_user.id, exc)


def save_managed_group(m):
    try:
        ref = normalize_chat_reference(m.text or "")
        chat = bot.get_chat(ref)
        if chat.type not in ("group", "supergroup"):
            raise ValueError("The selected chat must be a group or supergroup")
        member = bot.get_chat_member(chat.id, bot.get_me().id)
        if member.status not in ("administrator", "creator"):
            raise ValueError("Make the bot an administrator in that group first")
        groups = _managed_group_ids()
        if int(chat.id) not in groups:
            groups.append(int(chat.id))
            set_config("managed_chat_groups", groups)
        admin_success(m.from_user.id, f"Group management enabled for {chat.title} ({chat.id})")
        raw_bot.send_message(chat.id, "🛡 Group protection is now active. Free members cannot post promotional links, usernames or bot mentions. VIP members are exempt.")
    except Exception as exc:
        admin_error(m.from_user.id, exc)


def save_group_warning_limit(m):
    try:
        limit = int((m.text or "").strip())
        if limit < 1 or limit > 100:
            raise ValueError("Warning limit must be between 1 and 100")
        set_config("group_warning_limit", limit)
        admin_success(m.from_user.id, f"Warning limit set to {limit}")
    except Exception as exc:
        admin_error(m.from_user.id, exc)


@bot.callback_query_handler(func=lambda c: c.data.startswith("groupwarnaction|"))
def group_warning_action_cb(c):
    if not is_admin(c.from_user.id):
        return
    action = c.data.split("|", 1)[1]
    set_config("group_warning_action", action)
    if action == "mute":
        msg = raw_bot.send_message(c.from_user.id, "Send mute duration in minutes (example: 1440 for one day).")
        bot.register_next_step_handler(msg, save_group_mute_minutes)
    else:
        admin_success(c.from_user.id, "Warning action set to BAN")
    bot.answer_callback_query(c.id, "Updated")


def save_group_mute_minutes(m):
    try:
        minutes = int((m.text or "").strip())
        if minutes < 1:
            raise ValueError("Minutes must be at least 1")
        set_config("group_mute_minutes", minutes)
        admin_success(m.from_user.id, f"Warning action set to MUTE for {minutes} minutes")
    except Exception as exc:
        admin_error(m.from_user.id, exc)


def save_group_rules_text(m):
    text = (m.text or m.caption or "").strip()
    if not text:
        return admin_error(m.from_user.id, "Rules message cannot be empty")
    set_config("group_rules_text", text)
    msg = raw_bot.send_message(m.from_user.id, "Now send the button as:\nButton Name | @username-or-link\n\nSend skip for no button.")
    bot.register_next_step_handler(msg, save_group_rules_button)


def save_group_rules_button(m):
    raw = (m.text or "").strip()
    if raw.lower() == "skip":
        set_config("group_rules_button_text", "")
        set_config("group_rules_button_url", "")
        return admin_success(m.from_user.id, "Rules saved without a button")
    if "|" not in raw:
        return admin_error(m.from_user.id, "Use: Button Name | @username-or-link")
    label, target = [x.strip() for x in raw.split("|", 1)]
    if not label or not target:
        return admin_error(m.from_user.id, "Button name and target are required")
    if target.startswith("@"): target = "https://t.me/" + target[1:]
    elif not re.match(r"^https?://", target, re.I): target = "https://t.me/" + target.lstrip("@")
    set_config("group_rules_button_text", label[:64])
    set_config("group_rules_button_url", target)
    admin_success(m.from_user.id, "Rules and button saved")


def send_rules_group_picker(admin_id):
    groups = _managed_group_ids()
    if not groups:
        return admin_error(admin_id, "No managed groups configured")
    kb = InlineKeyboardMarkup(row_width=1)
    for gid in groups:
        try: title = bot.get_chat(gid).title or str(gid)
        except Exception: title = str(gid)
        kb.add(InlineKeyboardButton(f"📤 {title}", callback_data=f"groupsendrules|{gid}"))
    raw_bot.send_message(admin_id, "Select the group where rules should be posted:", reply_markup=kb)


@bot.callback_query_handler(func=lambda c: c.data.startswith("groupsendrules|"))
def send_rules_to_group_cb(c):
    if not is_admin(c.from_user.id): return
    try:
        gid = int(c.data.split("|", 1)[1])
        cfg = get_cached_config()
        text = cfg.get("group_rules_text", "").strip()
        if not text: raise ValueError("Set the rules message first")
        kb = None
        if cfg.get("group_rules_button_text") and cfg.get("group_rules_button_url"):
            kb = InlineKeyboardMarkup().add(InlineKeyboardButton(cfg["group_rules_button_text"], url=cfg["group_rules_button_url"]))
        raw_bot.send_message(gid, text, reply_markup=kb)
        bot.answer_callback_query(c.id, "Rules sent")
        admin_success(c.from_user.id, "Rules posted successfully")
    except Exception as exc:
        admin_error(c.from_user.id, exc)


def send_clear_messages_group_picker(admin_id):
    groups = _managed_group_ids()
    if not groups:
        return admin_error(admin_id, "No managed groups configured")
    kb = InlineKeyboardMarkup(row_width=1)
    for gid in groups:
        try: title = bot.get_chat(gid).title or str(gid)
        except Exception: title = str(gid)
        kb.add(InlineKeyboardButton(f"🧹 {title}", callback_data=f"groupcleartracked|{gid}"))
    raw_bot.send_message(admin_id, "Select a group. The bot can delete only messages it has tracked since this feature was enabled:", reply_markup=kb)


@bot.callback_query_handler(func=lambda c: c.data.startswith("groupcleartracked|"))
def clear_tracked_group_messages_cb(c):
    if not is_admin(c.from_user.id): return
    gid = int(c.data.split("|", 1)[1])
    rows = list(group_message_log_col.find({"group_id": gid}, {"message_id": 1}).sort("message_id", -1))
    deleted = failed = 0
    for row in rows:
        try:
            bot.delete_message(gid, int(row["message_id"]))
            deleted += 1
        except Exception:
            failed += 1
    group_message_log_col.delete_many({"group_id": gid})
    bot.answer_callback_query(c.id, "Cleanup complete")
    admin_success(c.from_user.id, f"Deleted {deleted} tracked messages. Failed: {failed}")


@bot.callback_query_handler(func=lambda c: c.data.startswith("groupmgrremove|"))
def remove_managed_group_callback(c):
    if not is_admin(c.from_user.id):
        return
    try:
        gid = int(c.data.split("|", 1)[1])
        groups = [x for x in _managed_group_ids() if x != gid]
        set_config("managed_chat_groups", groups)
        bot.answer_callback_query(c.id, "Removed")
        admin_success(c.from_user.id, f"Group removed from management: {gid}")
    except Exception as exc:
        admin_error(c.from_user.id, exc)


def notify_all_users_about_method(folder, action="published"):
    """Background notification after admin approval; avoids blocking approval flow."""
    if not get_cached_config().get("user_method_notifications_enabled", True):
        return
    name = str((folder or {}).get("name") or "New Method")
    cat = str((folder or {}).get("cat") or "method").upper()
    price = int((folder or {}).get("price") or 0)
    text = f"🚀 NEW {cat} METHOD\n\n📄 {name}\n💎 Price: {price} points\n\nOpen the bot to view it."
    def worker():
        for row in users_col.find({}, {"_id": 1}):
            try:
                raw_bot.send_message(int(row["_id"]), text, disable_notification=False)
                time.sleep(0.04)
            except Exception:
                continue
    threading.Thread(target=worker, daemon=True).start()

@bot.message_handler(
    func=lambda m: m.chat.type in ("group", "supergroup"),
    content_types=["text", "photo", "video", "document", "audio", "animation", "voice"],
)
@bot.channel_post_handler(content_types=["text", "photo", "video", "document", "audio", "animation", "voice"])
def auto_import_channel_post(m):
    _queue_auto_import(m, edited=False)
    if m.chat.type in ("group", "supergroup"):
        moderate_managed_group_message(m)


@bot.edited_message_handler(
    func=lambda m: m.chat.type in ("group", "supergroup"),
    content_types=["text", "photo", "video", "document", "audio", "animation", "voice"],
)
@bot.edited_channel_post_handler(content_types=["text", "photo", "video", "document", "audio", "animation", "voice"])
def auto_import_edited_post(m):
    _queue_auto_import(m, edited=True)

_pending_approval_state = {}


def pending_methods_keyboard(page=0, page_size=10):
    rows = list(pending_methods_col.find({"status": {"$in": ["pending", "pending_update", "pending_replace"]}}).sort("created_at", -1))
    kb = InlineKeyboardMarkup(row_width=1)
    chunk = rows[page * page_size:(page + 1) * page_size]
    for row in chunk:
        icon = "💎" if row.get("cat") == "vip" else "📂"
        kb.add(InlineKeyboardButton(f"{icon} {row.get('name')} • {len(row.get('files', []))} file(s)", callback_data=f"pendingview|{row['_id']}"))
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"pendingpage|{page-1}"))
    if (page + 1) * page_size < len(rows):
        nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"pendingpage|{page+1}"))
    if nav:
        kb.row(*nav)
    return kb, len(rows)


@bot.message_handler(func=lambda m: m.text == "⏳ Pending Methods" and is_admin(m.from_user.id))
def pending_methods_menu(m):
    kb, count = pending_methods_keyboard()
    raw_bot.send_message(m.from_user.id, f"⏳ PENDING METHODS\n\nWaiting for review: {count}\n\nSelect a method:", reply_markup=kb)


@bot.callback_query_handler(func=lambda c: c.data.startswith("pendingpage|"))
def pending_page_cb(c):
    if not is_admin(c.from_user.id):
        return
    page = int(c.data.split("|", 1)[1])
    kb, count = pending_methods_keyboard(page)
    raw_bot.edit_message_text(f"⏳ PENDING METHODS\n\nWaiting for review: {count}\n\nSelect a method:", c.from_user.id, c.message.message_id, reply_markup=kb)
    bot.answer_callback_query(c.id)


@bot.callback_query_handler(func=lambda c: c.data.startswith("pendingview|"))
def pending_view_cb(c):
    if not is_admin(c.from_user.id):
        return
    from bson import ObjectId
    try:
        pending_id = ObjectId(c.data.split("|", 1)[1])
        row = pending_methods_col.find_one({"_id": pending_id})
        if not row or row.get("status") not in ("pending", "pending_update", "pending_replace"):
            return bot.answer_callback_query(c.id, "This pending method is no longer available.", True)
        kb = InlineKeyboardMarkup(row_width=2)
        kb.add(
            InlineKeyboardButton("✅ Approve & Set Price", callback_data=f"pendingapprove|{pending_id}"),
            InlineKeyboardButton("❌ Reject", callback_data=f"pendingreject|{pending_id}"),
        )
        text = (
            "⏳ METHOD REVIEW\n\n"
            f"🏷 Name: {row.get('name')}\n"
            f"📁 Category: {str(row.get('cat')).upper()}\n"
            f"🏷 Source tag: #method{row.get('auto_method_key')}\n"
            f"📎 Files: {len(row.get('files', []))}\n"
            f"📝 Type: {row.get('status')}\n\n"
            "Approve to choose its point price."
        )
        raw_bot.send_message(c.from_user.id, text, reply_markup=kb)
        bot.answer_callback_query(c.id)
    except Exception as exc:
        bot.answer_callback_query(c.id, str(exc)[:180], True)


@bot.callback_query_handler(func=lambda c: c.data.startswith("pendingapprove|"))
def pending_approve_cb(c):
    if not is_admin(c.from_user.id):
        return
    pending_id = c.data.split("|", 1)[1]
    _pending_approval_state[c.from_user.id] = pending_id
    msg = raw_bot.send_message(c.from_user.id, "💰 Send the price in points for this method.\n\nSend 0 for a free method.")
    bot.register_next_step_handler(msg, pending_price_step)
    bot.answer_callback_query(c.id)


def pending_price_step(m):
    from bson import ObjectId
    try:
        pending_id = _pending_approval_state.pop(m.from_user.id, None)
        if not pending_id:
            raise ValueError("Approval session expired")
        price = int((m.text or "").strip())
        if price < 0:
            raise ValueError("Price cannot be negative")
        row = pending_methods_col.find_one({"_id": ObjectId(pending_id)})
        if not row or row.get("status") not in ("pending", "pending_update", "pending_replace"):
            raise ValueError("Pending method was already processed")

        status = row.get("status")
        existing = None
        if row.get("existing_folder_id"):
            existing = folders_col.find_one({"_id": row.get("existing_folder_id")})
        if not existing:
            existing = folders_col.find_one({"cat": row.get("cat"), "auto_method_key": row.get("auto_method_key")})

        if existing and status in ("pending_update", "pending_replace"):
            update = {"updated_at": now_ts()}
            if status == "pending_replace":
                update.update({"name": row.get("name"), "files": row.get("files", []), "price": price})
            else:
                update.update({"price": price})
            if status == "pending_update":
                folders_col.update_one({"_id": existing["_id"]}, {"$push": {"files": {"$each": row.get("files", [])}}, "$set": update})
            else:
                folders_col.update_one({"_id": existing["_id"]}, {"$set": update})
            folder = folders_col.find_one({"_id": existing["_id"]})
            action = "updated"
        else:
            number = fs.add(row.get("cat"), row.get("name"), row.get("files", []), price, at_start=True)
            folders_col.update_one({"number": number}, {"$set": {
                "auto_method_key": row.get("auto_method_key"), "source_chat": row.get("source_chat"),
                "approved_by": m.from_user.id, "approved_at": now_ts(), "pinned": False,
                "parent": None, "sort_priority": -time.time(),
            }})
            folder = fs.get_by_number(number)
            append_to_manual_methods_list(folder or {"name": row.get("name"), "cat": row.get("cat")})
            action = "approved and published"

        pending_methods_col.update_one({"_id": row["_id"]}, {"$set": {"status": "approved", "price": price, "approved_by": m.from_user.id, "approved_at": now_ts()}})
        send_method_notification(action, folder)
        notify_all_users_about_method(folder, action)
        raw_bot.send_message(
            m.from_user.id,
            f"✅ Process Complete\n\n{row.get('name')} has been {action}.\n💰 Price: {price} points\n📌 It appears below pinned methods and above older unpinned methods.",
            reply_markup=admin_menu(),
        )
        try:
            if get_config().get("group_import_notify_enabled", True):
                raw_bot.send_message(row.get("source_chat"), f"✅ {row.get('name')} was approved and published at {price} points.")
        except Exception:
            pass
    except Exception as exc:
        _pending_approval_state.pop(m.from_user.id, None)
        admin_error(m.from_user.id, exc)


@bot.callback_query_handler(func=lambda c: c.data.startswith("pendingreject|"))
def pending_reject_cb(c):
    if not is_admin(c.from_user.id):
        return
    from bson import ObjectId
    try:
        pending_id = ObjectId(c.data.split("|", 1)[1])
        row = pending_methods_col.find_one_and_update(
            {"_id": pending_id, "status": {"$in": ["pending", "pending_update", "pending_replace"]}},
            {"$set": {"status": "rejected", "rejected_by": c.from_user.id, "rejected_at": now_ts()}},
        )
        if not row:
            raise ValueError("Method was already processed")
        bot.answer_callback_query(c.id, "Rejected")
        raw_bot.send_message(c.from_user.id, f"✅ Process Complete\nRejected: {row.get('name')}", reply_markup=admin_menu())
        try:
            if get_config().get("group_import_notify_enabled", True):
                raw_bot.send_message(row.get("source_chat"), f"❌ {row.get('name')} was rejected by admin and was not published.")
        except Exception:
            pass
    except Exception as exc:
        admin_error(c.from_user.id, exc)


def pin_methods_keyboard(action, page=0, page_size=12):
    query = {"parent": None, "pinned": {"$ne": True}} if action == "pin" else {"parent": None, "pinned": True}
    rows = list(folders_col.find(query).sort([("pinned", -1), ("sort_priority", 1), ("number", 1)]))
    kb = InlineKeyboardMarkup(row_width=1)
    for row in rows[page * page_size:(page + 1) * page_size]:
        icon = "📌" if row.get("pinned") else "📄"
        kb.add(InlineKeyboardButton(f"{icon} {row.get('name')}", callback_data=f"methodpinset|{action}|{row['_id']}"))
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️", callback_data=f"methodpinpage|{action}|{page-1}"))
    if (page + 1) * page_size < len(rows):
        nav.append(InlineKeyboardButton("➡️", callback_data=f"methodpinpage|{action}|{page+1}"))
    if nav:
        kb.row(*nav)
    return kb, len(rows)


@bot.message_handler(func=lambda m: m.text == "📌 Pin Methods" and is_admin(m.from_user.id))
def pin_methods_menu(m):
    kb = InlineKeyboardMarkup(row_width=2)
    kb.row(InlineKeyboardButton("📂 Pin FREE", callback_data="pinv2cat|free|pin"), InlineKeyboardButton("💎 Pin VIP", callback_data="pinv2cat|vip|pin"))
    kb.row(InlineKeyboardButton("📂 Unpin FREE", callback_data="pinv2cat|free|unpin"), InlineKeyboardButton("💎 Unpin VIP", callback_data="pinv2cat|vip|unpin"))
    raw_bot.send_message(m.from_user.id, "📌 METHOD PLACEMENT\n\nFREE and VIP pins are managed separately. You can pin multiple methods in each category.", reply_markup=kb)


@bot.callback_query_handler(func=lambda c: c.data.startswith("methodpinmenu|"))
def method_pin_menu_cb(c):
    if not is_admin(c.from_user.id):
        return
    action = c.data.split("|", 1)[1]
    kb, count = pin_methods_keyboard(action)
    bot.send_message(c.from_user.id, f"{'📌 Select a method to pin' if action == 'pin' else '📍 Select a method to unpin'}\n\nAvailable: {count}", reply_markup=kb)
    bot.answer_callback_query(c.id)


@bot.callback_query_handler(func=lambda c: c.data.startswith("methodpinpage|"))
def method_pin_page_cb(c):
    if not is_admin(c.from_user.id):
        return
    _, action, page = c.data.split("|")
    kb, count = pin_methods_keyboard(action, int(page))
    bot.edit_message_reply_markup(c.from_user.id, c.message.message_id, reply_markup=kb)
    bot.answer_callback_query(c.id)


@bot.callback_query_handler(func=lambda c: c.data.startswith("methodpinset|"))
def method_pin_set_cb(c):
    if not is_admin(c.from_user.id):
        return
    from bson import ObjectId
    try:
        _, action, object_id = c.data.split("|")
        value = action == "pin"
        row = folders_col.find_one_and_update({"_id": ObjectId(object_id)}, {"$set": {"pinned": value, "pinned_at": now_ts() if value else None, "pinned_by": c.from_user.id if value else None}}, return_document=ReturnDocument.AFTER)
        if not row:
            raise ValueError("Method not found")
        bot.answer_callback_query(c.id, "Pinned" if value else "Unpinned", True)
        raw_bot.send_message(c.from_user.id, f"✅ Process Complete\n{'📌 Pinned' if value else '📍 Unpinned'}: {row.get('name')}", reply_markup=admin_menu())
    except Exception as exc:
        admin_error(c.from_user.id, exc)


def scheduler_loop():
    while not _scheduler_stop.wait(20):
        try:
            now=now_ts()
            for x in broadcasts_col.find({'status':'scheduled','run_at':{'$lte':now}}).limit(10):
                q={} if x['target']=='all' else {'vip':x['target']=='vip'}; sent=failed=0
                for u in users_col.find(q,{'_id':1}):
                    try: bot.copy_message(int(u['_id']),x['source_chat'],x['source_message']); sent+=1
                    except Exception: failed+=1
                broadcasts_col.update_one({'_id':x['_id']},{'$set':{'status':'sent','sent':sent,'failed':failed,'sent_at':now}}); log_event('broadcast_sent',x.get('created_by'),details={'sent':sent,'failed':failed})
            group_auto = _group_auto_config()
            if group_auto.get('active') and group_auto.get('next_run') and group_auto.get('next_run') <= now:
                sent, failed = _send_group_auto_message(group_auto)
                interval_minutes = max(int(group_auto.get('interval_minutes', 60) or 60), 5)
                _save_group_auto(last_run=now, next_run=now + interval_minutes * 60)
                log_event('group_auto_message_sent', details={'sent': sent, 'failed': failed[:10]})
            for x in auto_posts_col.find({'active':True,'next_run':{'$lte':now}}).limit(20):
                channels = x.get('channels') or [x.get('channel')]
                previous = x.get('last_message_ids') or {}
                new_message_ids = dict(previous)
                for target in channels:
                    try:
                        _delete_previous_auto_messages(target, previous.get(str(target), []))
                        if x.get('payload'):
                            new_message_ids[str(target)] = _send_payload(target, x['payload'])
                        else:
                            new_message_ids[str(target)] = [bot.copy_message(target, x['source_chat'], x['source_message']).message_id]
                        log_event('auto_post_sent', target=target)
                    except Exception as exc:
                        log_event('auto_post_error', target=target, details={'error':str(exc)}, level='error')
                if x['schedule'] == 'every_hours':
                    nxt = now + max(float(x['value']), 0.01) * 3600
                else:
                    hh, mm = map(int, str(x['value']).split(':'))
                    local_now = datetime.utcfromtimestamp(now + TZ_OFFSET_SECONDS)
                    local_next = local_now.replace(hour=hh, minute=mm, second=0, microsecond=0)
                    if local_next <= local_now:
                        local_next += timedelta(days=1)
                    nxt = local_next.timestamp() - TZ_OFFSET_SECONDS
                auto_posts_col.update_one({'_id':x['_id']},{'$set':{'next_run':nxt, 'last_run':now, 'last_message_ids':new_message_ids}})
        except Exception as exc: log_event('scheduler_error',details={'error':str(exc)},level='error')

threading.Thread(target=scheduler_loop,name='zedox-scheduler',daemon=True).start()



@bot.callback_query_handler(func=lambda c: c.data.startswith("buyid|"))
def buy_method_by_id(c):
    from bson import ObjectId
    try:
        _, oid, price_text = c.data.split("|", 2)
        folder = folders_col.find_one({"_id": ObjectId(oid)})
        if not folder:
            return bot.answer_callback_query(c.id, "Method not found", True)
        if folder.get("expired"):
            return bot.answer_callback_query(c.id, "This method has expired", True)
        user = User(c.from_user.id)
        price = int(price_text)
        name = folder.get("name", "Method")
        if user.is_vip() or user.can_access_method(name):
            return bot.answer_callback_query(c.id, "You already have access", True)
        if user.points() < price:
            return bot.answer_callback_query(c.id, f"Need {price} points. You have {user.points()}", True)
        if not user.purchase_method(name, price):
            raise ValueError("Purchase could not be completed")
        raw_bot.edit_message_text(
            f"🎉 PURCHASE COMPLETE\n\n📄 {name}\n💰 Paid: {price} points\n💎 Remaining: {user.points()} points\n\nOpen the method again to receive its content.",
            c.from_user.id, c.message.message_id
        )
        bot.answer_callback_query(c.id, "Purchased successfully")
    except Exception as exc:
        bot.answer_callback_query(c.id, "Purchase failed", True)
        log_event("buy_method_id_error", c.from_user.id, details={"error": str(exc)}, level="error")

# =========================
# ✅ STABILITY FIXES: SAFE METHOD OPEN, BUTTON VISIBILITY, PINS, NOTIFICATIONS
# =========================
@bot.callback_query_handler(func=lambda c: c.data.startswith("openid|"))
def open_folder_by_id(c):
    if force_block(c.from_user.id):
        return
    try:
        from bson import ObjectId
        folder = folders_col.find_one({"_id": ObjectId(c.data.split("|", 1)[1])})
        if not folder:
            return bot.answer_callback_query(c.id, "Method not found", True)
        if folder.get("expired"):
            bot.answer_callback_query(c.id, "⛔ This method has expired", True)
            raw_bot.send_message(c.from_user.id, f"⛔ METHOD EXPIRED\n\n{folder.get('name', 'This method')} is unavailable and cannot be opened or purchased.")
            return
        # Reuse the existing handler with a safe legacy payload.
        fake = type("SafeCallback", (), {})()
        fake.from_user = c.from_user
        fake.id = c.id
        fake.message = c.message
        fake.data = f"open|{folder.get('cat')}|{folder.get('name')}|{folder.get('parent') or ''}"
        open_folder(fake)
    except Exception as exc:
        bot.answer_callback_query(c.id, "Unable to open method", True)
        log_event("open_method_error", c.from_user.id, details={"error": str(exc)}, level="error")

@bot.callback_query_handler(func=lambda c: c.data.startswith("openbyname|"))
def open_folder_by_name(c):
    try:
        _, cat, name = c.data.split("|", 2)
        row = folders_col.find_one({"cat": cat, "name": name})
        if not row:
            return bot.answer_callback_query(c.id, "Method not found", True)
        c.data = f"openid|{row['_id']}"
        return open_folder_by_id(c)
    except Exception as exc:
        bot.answer_callback_query(c.id, "Unable to open method", True)

def _all_visibility_candidates():
    result = list(MAIN_MENU_BUTTONS)
    for item in get_custom_buttons():
        text = str(item.get("text") or "").strip()
        if text and text not in result:
            result.append(text)
    return result

def _visibility_keyboard(mode, page=0, page_size=12):
    hidden = set(get_config().get("hidden_main_buttons", []) or [])
    all_items = _all_visibility_candidates()
    items = [x for x in all_items if (x not in hidden if mode == "hide" else x in hidden)]
    kb = InlineKeyboardMarkup(row_width=1)
    page_items = items[page*page_size:(page+1)*page_size]
    for absolute_index, text in enumerate(page_items, start=page*page_size):
        icon = "🙈" if mode == "hide" else "👁"
        kb.add(InlineKeyboardButton(f"{icon} {text}", callback_data=f"vis2|{mode}|{absolute_index}|{page}"))
    nav=[]
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️", callback_data=f"vis2page|{mode}|{page-1}"))
    if (page+1)*page_size < len(items):
        nav.append(InlineKeyboardButton("➡️", callback_data=f"vis2page|{mode}|{page+1}"))
    if nav: kb.row(*nav)
    kb.row(InlineKeyboardButton("🔄 Refresh", callback_data=f"vis2page|{mode}|{page}"))
    return kb, len(items)

@bot.message_handler(func=lambda m: m.text in ("🙈 Hide Button", "👁 Show Button") and is_admin(m.from_user.id))
def visibility_menu(m):
    mode = "hide" if m.text.startswith("🙈") else "show"
    kb, count = _visibility_keyboard(mode)
    title = "🙈 HIDE USER BUTTONS" if mode == "hide" else "👁 SHOW USER BUTTONS"
    note = "Choose a visible button to hide." if mode == "hide" else "Choose a hidden button to restore."
    raw_bot.send_message(m.from_user.id, f"{title}\n━━━━━━━━━━━━━━━━━━━━\n{note}\n\nAvailable: {count}", reply_markup=kb)

@bot.callback_query_handler(func=lambda c: c.data.startswith("vis2page|"))
def visibility_page_callback(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admins only", True)
    try:
        _, mode, page = c.data.split("|")
        kb, count = _visibility_keyboard(mode, int(page))
        bot.edit_message_reply_markup(c.from_user.id, c.message.message_id, reply_markup=kb)
        bot.answer_callback_query(c.id, f"{count} available")
    except Exception as exc:
        admin_error(c.from_user.id, exc)

@bot.callback_query_handler(func=lambda c: c.data.startswith("vis2|"))
def visibility_callback_v2(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admins only", True)
    try:
        _, mode, index, page = c.data.split("|")
        hidden = list(get_config().get("hidden_main_buttons", []) or [])
        hidden_set = set(hidden)
        candidates = [x for x in _all_visibility_candidates() if (x not in hidden_set if mode == "hide" else x in hidden_set)]
        idx = int(index)
        if idx < 0 or idx >= len(candidates):
            raise ValueError("Button list changed. Open Hide/Show again")
        text = candidates[idx]
        if mode == "hide":
            if text not in hidden:
                hidden.append(text)
        else:
            hidden = [x for x in hidden if x != text]
        set_config("hidden_main_buttons", hidden)
        # Force immediate cache refresh on every worker path.
        global _config_cache, _config_cache_time
        _config_cache = None
        _config_cache_time = 0
        bot.answer_callback_query(c.id, "Button hidden" if mode == "hide" else "Button restored", True)
        kb, _ = _visibility_keyboard(mode, int(page))
        try:
            bot.edit_message_reply_markup(c.from_user.id, c.message.message_id, reply_markup=kb)
        except Exception:
            pass
        raw_bot.send_message(c.from_user.id, f"✅ Button {'hidden' if mode == 'hide' else 'shown'}: {text}\n\nUsers receive the updated menu on their next bot action or /start.", reply_markup=admin_menu())
    except Exception as exc:
        admin_error(c.from_user.id, exc)

def pin_methods_keyboard_v2(cat, action, page=0, page_size=12):
    query = {"parent": None, "cat": cat}
    query["pinned"] = {"$ne": True} if action == "pin" else True
    rows = list(folders_col.find(query).sort([("pinned", -1), ("pinned_at", -1), ("created_at", -1)]))
    kb = InlineKeyboardMarkup(row_width=1)
    for row in rows[page*page_size:(page+1)*page_size]:
        kb.add(InlineKeyboardButton(("📌 " if row.get("pinned") else "📄 ") + row.get("name", "Unnamed"), callback_data=f"pinv2set|{cat}|{action}|{row['_id']}"))
    nav=[]
    if page>0: nav.append(InlineKeyboardButton("⬅️", callback_data=f"pinv2page|{cat}|{action}|{page-1}"))
    if (page+1)*page_size < len(rows): nav.append(InlineKeyboardButton("➡️", callback_data=f"pinv2page|{cat}|{action}|{page+1}"))
    if nav: kb.row(*nav)
    return kb, len(rows)

# Replace the old Pin Methods experience with category-specific controls.
@bot.callback_query_handler(func=lambda c: c.data.startswith("pinv2cat|"))
def pin_v2_category(c):
    if not is_admin(c.from_user.id): return
    _, cat, action = c.data.split("|")
    kb, count = pin_methods_keyboard_v2(cat, action)
    raw_bot.send_message(c.from_user.id, f"{'FREE' if cat == 'free' else 'VIP'} methods — select one to {'pin' if action == 'pin' else 'unpin'} ({count} available):", reply_markup=kb)
    bot.answer_callback_query(c.id)

@bot.callback_query_handler(func=lambda c: c.data.startswith("pinv2page|"))
def pin_v2_page(c):
    _, cat, action, page = c.data.split("|")
    kb, _ = pin_methods_keyboard_v2(cat, action, int(page))
    bot.edit_message_reply_markup(c.from_user.id, c.message.message_id, reply_markup=kb)
    bot.answer_callback_query(c.id)

@bot.callback_query_handler(func=lambda c: c.data.startswith("pinv2set|"))
def pin_v2_set(c):
    if not is_admin(c.from_user.id): return
    try:
        from bson import ObjectId
        _, cat, action, oid = c.data.split("|")
        value = action == "pin"
        row = folders_col.find_one_and_update({"_id": ObjectId(oid), "cat": cat}, {"$set": {"pinned": value, "pinned_at": now_ts() if value else None, "pinned_by": c.from_user.id if value else None}}, return_document=ReturnDocument.AFTER)
        if not row: raise ValueError("Method not found")
        bot.answer_callback_query(c.id, "Pinned" if value else "Unpinned", True)
        admin_success(c.from_user.id, f"{'Pinned' if value else 'Unpinned'} {cat.upper()} method: {row.get('name')}")
    except Exception as exc:
        admin_error(c.from_user.id, exc)

def _notify_settings_keyboard():
    cfg = get_config()
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton(f"🔔 Published method alerts: {'ON' if cfg.get('method_notify_enabled', True) else 'OFF'}", callback_data="notifyv2|toggle"))
    kb.add(InlineKeyboardButton(f"📥 Source group replies: {'ON' if cfg.get('group_import_notify_enabled', True) else 'OFF'}", callback_data="notifyv2|togglegroup"))
    kb.add(InlineKeyboardButton("🧪 Send test notification", callback_data="notifyv2|test"))
    kb.add(InlineKeyboardButton("👥 Notification groups", callback_data="notifyv2|groups"))
    return kb

def _send_notify_settings(uid):
    cfg = get_config()
    target = cfg.get("method_notify_group") or cfg.get("join_notify_group") or "Not set"
    raw_bot.send_message(uid, f"🔔 NOTIFICATION SETTINGS\n\nMethod upload/update alerts can be switched on or off.\nTarget group: {target}", reply_markup=_notify_settings_keyboard())

@bot.message_handler(func=lambda m: m.text == "🔔 Notify" and is_admin(m.from_user.id))
def notify_settings_v2(m):
    _send_notify_settings(m.from_user.id)

@bot.callback_query_handler(func=lambda c: c.data.startswith("notifyv2|"))
def notify_v2_callback(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admins only", True)
    action = c.data.split("|", 1)[1]
    try:
        if action == "toggle":
            cfg = get_config()
            new = not cfg.get("method_notify_enabled", True)
            set_config("method_notify_enabled", new)
            bot.answer_callback_query(c.id, "Notifications ON" if new else "Notifications OFF", True)
            try:
                bot.edit_message_reply_markup(c.from_user.id, c.message.message_id, reply_markup=_notify_settings_keyboard())
            except Exception:
                _send_notify_settings(c.from_user.id)
            return
        if action == "togglegroup":
            cfg = get_config()
            new = not cfg.get("group_import_notify_enabled", True)
            set_config("group_import_notify_enabled", new)
            bot.answer_callback_query(c.id, "Source group replies ON" if new else "Source group replies OFF", True)
            try:
                bot.edit_message_reply_markup(c.from_user.id, c.message.message_id, reply_markup=_notify_settings_keyboard())
            except Exception:
                _send_notify_settings(c.from_user.id)
            return
        if action == "groups":
            bot.answer_callback_query(c.id)
            raw_bot.send_message(c.from_user.id, "Choose notification settings:", reply_markup=join_notification_keyboard())
            return
        if action == "test":
            cfg = get_config()
            if not cfg.get("method_notify_enabled", True):
                raise ValueError("Method notifications are OFF. Turn them ON first.")
            target = cfg.get("method_notify_group") or cfg.get("join_notify_group")
            if not target:
                raise ValueError("Set a method notification group first")
            raw_bot.send_message(target, "🔔 Test successful! Method upload and update notifications are working.")
            bot.answer_callback_query(c.id, "Test sent", True)
            admin_success(c.from_user.id, "Test notification sent successfully")
            return
    except Exception as exc:
        bot.answer_callback_query(c.id, "Failed", True)
        admin_error(c.from_user.id, exc)



# =========================
# 🔓 USER RESTRICTION MANAGEMENT
# =========================
def send_restricted_users_picker(admin_id):
    records = list(group_warnings_col.find({"action_taken": {"$exists": True, "$ne": None}}).sort("action_at", -1).limit(100))
    if not records:
        return raw_bot.send_message(admin_id, "✅ No muted or banned users are currently recorded.")
    kb = InlineKeyboardMarkup(row_width=1)
    for rec in records:
        gid = int(rec.get("group_id"))
        uid = int(rec.get("user_id"))
        username = rec.get("username")
        name = rec.get("name") or "User"
        action = str(rec.get("action_taken", "restricted"))
        label = f"{'@'+username if username else name} • {action}"
        kb.add(InlineKeyboardButton(label[:60], callback_data=f"restoremember|{gid}|{uid}"))
    raw_bot.send_message(admin_id, "🔓 SELECT A USER\n\nChoose a person to unmute or unban:", reply_markup=kb)


@bot.callback_query_handler(func=lambda c: c.data.startswith("restoremember|"))
def restore_group_member_cb(c):
    if not is_admin(c.from_user.id):
        return
    try:
        _, gid_s, uid_s = c.data.split("|", 2)
        gid, uid = int(gid_s), int(uid_s)
        rec = group_warnings_col.find_one({"group_id": gid, "user_id": uid}) or {}
        action = str(rec.get("action_taken", "")).lower()
        if "ban" in action:
            bot.unban_chat_member(gid, uid, only_if_banned=True)
            result = "unbanned"
        else:
            bot.restrict_chat_member(
                gid, uid,
                can_send_messages=True,
                can_send_audios=True,
                can_send_documents=True,
                can_send_photos=True,
                can_send_videos=True,
                can_send_video_notes=True,
                can_send_voice_notes=True,
                can_send_polls=True,
                can_send_other_messages=True,
                can_add_web_page_previews=True,
                can_change_info=False,
                can_invite_users=True,
                can_pin_messages=False,
                can_manage_topics=False,
            )
            result = "unmuted"
        group_warnings_col.update_one(
            {"group_id": gid, "user_id": uid},
            {"$unset": {"action_taken": "", "action_at": ""}, "$set": {"warnings": 0, "restored_at": now_ts(), "restored_by": c.from_user.id}},
        )
        username = rec.get("username")
        display = f"@{username}" if username else rec.get("name") or str(uid)
        bot.answer_callback_query(c.id, f"User {result}", True)
        admin_success(c.from_user.id, f"{display} was successfully {result} in group {gid}")
    except Exception as exc:
        bot.answer_callback_query(c.id, "Failed", True)
        admin_error(c.from_user.id, exc)


# =========================
# 🚨 SCAM REPORT SYSTEM
# =========================
_scam_report_sessions = {}

def _clean_username(value):
    value = (value or "").strip()
    if value.startswith("https://t.me/"):
        value = value.split("https://t.me/", 1)[1].split("/", 1)[0]
    return value.lstrip("@").strip().lower()


def _extract_report_target(message):
    # Replying to a user's message is the most reliable option.
    reply = getattr(message, "reply_to_message", None)
    if reply and getattr(reply, "from_user", None):
        u = reply.from_user
        return u.id, (u.username or "").lower(), (u.first_name or "User")
    text = (message.text or "").strip()
    parts = text.split(maxsplit=1)
    arg = parts[1].strip() if len(parts) > 1 else ""
    # Also accept /scammer@username as requested, except when suffix is this bot's own username.
    first = parts[0]
    if not arg and first.lower().startswith("/scammer@"):
        suffix = first.split("@", 1)[1]
        try:
            if suffix.lower() != bot.get_me().username.lower():
                arg = "@" + suffix
        except Exception:
            arg = "@" + suffix
    username = _clean_username(arg)
    if not username:
        return None, None, None
    known = users_col.find_one({"username": {"$regex": f"^{re.escape(username)}$", "$options": "i"}})
    return (int(known["_id"]) if known and str(known.get("_id", "")).isdigit() else None), username, (known.get("first_name") if known else username)


def _scam_status_label(approved, pending):
    if approved > 0:
        return "🚨 DECLARED / APPROVED REPORTS"
    if pending > 0:
        return "⏳ REPORTS PENDING ADMIN REVIEW"
    return "✅ NO APPROVED SCAM DECLARATION"


@bot.message_handler(func=lambda m: bool(m.text) and (m.text.lower() == "/scammer" or m.text.lower().startswith("/scammer ") or m.text.lower().startswith("/scammer@")))
def scammer_report_cmd(m):
    try:
        target_id, username, target_name = _extract_report_target(m)
        if not username and not target_id:
            return raw_bot.send_message(
                m.chat.id,
                "🚨 REPORT A POSSIBLE SCAMMER\n\n"
                "Reply to the person's message with /scammer\n"
                "or send /scammer @username\n\n"
                "Then send the reason and any screenshot, video, document, forwarded message, or text proof.",
                reply_to_message_id=m.message_id,
            )
        if target_id == m.from_user.id or (username and m.from_user.username and username == m.from_user.username.lower()):
            return raw_bot.send_message(m.chat.id, "❌ You cannot report yourself.", reply_to_message_id=m.message_id)
        _scam_report_sessions[m.from_user.id] = {
            "target_id": target_id,
            "username": username or "",
            "target_name": target_name or username or "Unknown",
            "chat_id": m.chat.id,
            "started_at": now_ts(),
        }
        raw_bot.send_message(
            m.chat.id,
            "📝 SCAM REPORT DETAILS\n\n"
            f"Target: {'@'+username if username else target_name}\n\n"
            "Now send one evidence message. It can be text, a photo, video, document, audio, or a forwarded message.\n\n"
            "Send /cancel to stop.",
            reply_to_message_id=m.message_id,
        )
    except Exception as exc:
        raw_bot.send_message(m.chat.id, f"❌ Could not start the report: {str(exc)[:500]}")


@bot.message_handler(func=lambda m: m.from_user is not None and m.from_user.id in _scam_report_sessions, content_types=["text", "photo", "video", "document", "audio", "voice", "animation", "video_note", "sticker"])
def save_scam_report_details(m):
    session = _scam_report_sessions.get(m.from_user.id)
    if not session:
        return
    if (m.text or "").strip().lower() == "/cancel":
        _scam_report_sessions.pop(m.from_user.id, None)
        return raw_bot.send_message(m.chat.id, "❌ Scam report cancelled.")
    try:
        details = (m.text or m.caption or f"{m.content_type} evidence attached").strip()
        report = {
            "reporter_id": int(m.from_user.id),
            "reporter_username": (m.from_user.username or "").lower(),
            "target_user_id": session.get("target_id"),
            "target_username": (session.get("username") or "").lower(),
            "target_name": session.get("target_name") or "Unknown",
            "details": details,
            "evidence_chat_id": int(m.chat.id),
            "evidence_message_id": int(m.message_id),
            "evidence_type": m.content_type,
            "status": "pending",
            "created_at": now_ts(),
        }
        result = scam_reports_col.insert_one(report)
        rid = str(result.inserted_id)
        _scam_report_sessions.pop(m.from_user.id, None)
        kb = InlineKeyboardMarkup(row_width=2)
        kb.add(
            InlineKeyboardButton("✅ Approve Report", callback_data=f"scamapprove|{rid}"),
            InlineKeyboardButton("❌ Reject Report", callback_data=f"scamreject|{rid}"),
        )
        target_display = f"@{report['target_username']}" if report.get("target_username") else str(report.get("target_user_id") or report.get("target_name"))
        reporter_display = f"@{m.from_user.username}" if m.from_user.username else f"ID {m.from_user.id}"
        admin_text = (
            "🚨 NEW SCAM REPORT\n\n"
            f"Target: {target_display}\n"
            f"Reporter: {reporter_display}\n"
            f"Evidence type: {m.content_type}\n"
            f"Details: {details[:1200]}\n"
            f"Report ID: {rid}"
        )
        delivered = 0
        for admin in get_all_admins():
            try:
                admin_id = int(admin["_id"])
                raw_bot.send_message(admin_id, admin_text, reply_markup=kb)
                try:
                    raw_bot.copy_message(admin_id, m.chat.id, m.message_id)
                except Exception:
                    pass
                delivered += 1
            except Exception as exc:
                log_event("scam_report_admin_delivery_error", m.from_user.id, admin.get("_id"), {"error": str(exc)}, level="error")
        if delivered == 0:
            scam_reports_col.delete_one({"_id": result.inserted_id})
            raise RuntimeError("The report could not be delivered to any admin.")
        raw_bot.send_message(
            m.chat.id,
            "✅ REPORT SUBMITTED\n\n"
            "Your evidence was saved and sent to the admins.\n"
            "Status: ⏳ Pending review\n\n"
            "You will receive a message after approval or rejection.",
        )
    except Exception as exc:
        _scam_report_sessions.pop(m.from_user.id, None)
        raw_bot.send_message(m.chat.id, f"❌ Could not submit the report: {str(exc)[:700]}")


@bot.callback_query_handler(func=lambda c: c.data.startswith("scamapprove|") or c.data.startswith("scamreject|"))
def scam_report_review_cb(c):
    if not is_admin(c.from_user.id):
        return
    try:
        action, rid = c.data.split("|", 1)
        from bson import ObjectId
        report = scam_reports_col.find_one({"_id": ObjectId(rid)})
        if not report:
            raise ValueError("Report not found")
        status = "approved" if action == "scamapprove" else "rejected"
        scam_reports_col.update_one(
            {"_id": report["_id"]},
            {"$set": {"status": status, "reviewed_at": now_ts(), "reviewed_by": c.from_user.id}},
        )
        target = f"@{report.get('target_username')}" if report.get("target_username") else str(report.get("target_user_id") or report.get("target_name"))
        bot.answer_callback_query(c.id, f"Report {status}", True)
        raw_bot.edit_message_text(
            f"{'🚨 APPROVED SCAM REPORT' if status == 'approved' else '❌ REJECTED REPORT'}\n\nTarget: {target}\nReviewed by admin.",
            c.message.chat.id,
            c.message.message_id,
        )
        try:
            raw_bot.send_message(int(report["reporter_id"]), f"📋 REPORT UPDATE\n\nYour report about {target} was {status.upper()} by an admin.")
        except Exception:
            pass
    except Exception as exc:
        bot.answer_callback_query(c.id, "Failed", True)
        admin_error(c.from_user.id, exc)


@bot.message_handler(commands=["scammerlist"])
def scammer_list_cmd(m):
    pipeline = [
        {"$match": {"status": {"$in": ["pending", "approved"]}}},
        {"$group": {
            "_id": {"username": "$target_username", "user_id": "$target_user_id", "name": "$target_name"},
            "approved": {"$sum": {"$cond": [{"$eq": ["$status", "approved"]}, 1, 0]}},
            "pending": {"$sum": {"$cond": [{"$eq": ["$status", "pending"]}, 1, 0]}},
            "total": {"$sum": 1},
        }},
        {"$sort": {"approved": -1, "pending": -1, "total": -1}},
        {"$limit": 100},
    ]
    rows = list(scam_reports_col.aggregate(pipeline))
    if not rows:
        return raw_bot.send_message(m.chat.id, "🛡 SCAMMER RECORDS\n\n✅ No scam reports have been submitted yet.")
    chunks, current = [], "🚨 SCAMMER REPORT LIST\n\n"
    for i, row in enumerate(rows, 1):
        key = row["_id"]
        display = f"@{key.get('username')}" if key.get("username") else key.get("name") or str(key.get("user_id"))
        declared = "🚨 ADMIN DECLARED" if row["approved"] else "⏳ NOT DECLARED — PENDING REVIEW"
        block = f"{i}. {display}\n   {declared}\n   Approved: {row['approved']} | Pending: {row['pending']}\n\n"
        if len(current) + len(block) > 3900:
            chunks.append(current); current = ""
        current += block
    if current: chunks.append(current)
    for chunk in chunks:
        raw_bot.send_message(m.chat.id, chunk)


@bot.message_handler(commands=["check"])
def scam_check_standard_cmd(m):
    if getattr(m, "reply_to_message", None) and getattr(m.reply_to_message, "from_user", None):
        username = (m.reply_to_message.from_user.username or "").lower()
        if not username:
            return raw_bot.send_message(m.chat.id, "❌ That user has no public username, so username-based scam records cannot be checked.")
        m.text = f"/check @{username}"
        return scam_check_cmd(m)
    if not (m.text or "").strip().lower().startswith("/check "):
        return raw_bot.send_message(m.chat.id, "🔎 SCAM CHECK\n\nUse /check @username, or reply to a user's message with /check.")
    return scam_check_cmd(m)


@bot.message_handler(func=lambda m: bool(m.text) and (m.text.lower().startswith("/check ") or (m.text.lower().startswith("/check@") and not m.text.lower().startswith("/check@" + (bot.get_me().username or "").lower()))))
def scam_check_cmd(m):
    text = (m.text or "").strip()
    if text.lower().startswith("/check "):
        target = text.split(maxsplit=1)[1]
    else:
        target = "@" + text.split("@", 1)[1].split()[0]
    username = _clean_username(target)
    if not username:
        return raw_bot.send_message(m.chat.id, "🔎 Use /check @username")
    approved = scam_reports_col.count_documents({"target_username": username, "status": "approved"})
    pending = scam_reports_col.count_documents({"target_username": username, "status": "pending"})
    rejected = scam_reports_col.count_documents({"target_username": username, "status": "rejected"})
    if approved == 0 and pending == 0:
        return raw_bot.send_message(
            m.chat.id,
            f"🛡 SCAM CHECK\n\nUser: @{username}\n\n✅ No scam report is currently recorded for this username.\n\nStay careful and always verify payments independently.",
        )
    status = _scam_status_label(approved, pending)
    raw_bot.send_message(
        m.chat.id,
        f"🚨 SCAM CHECK RESULT\n\nUser: @{username}\nStatus: {status}\n\nApproved reports: {approved}\nPending reports: {pending}\nRejected reports: {rejected}\n\nAdmin declaration is based only on reviewed reports stored in this bot.",
    )


# =========================
# 📢 USER CHANNEL PROMOTION
# =========================
_channel_submit_sessions = {}

def _normalize_channel_reference(value):
    value = (value or "").strip()
    if not value:
        return None, None
    # Public t.me links only. Strip query/path noise.
    value = value.replace("https://telegram.me/", "https://t.me/")
    value = value.replace("http://telegram.me/", "https://t.me/")
    value = value.replace("http://t.me/", "https://t.me/")
    if value.startswith("https://t.me/"):
        tail = value.split("https://t.me/", 1)[1].split("?", 1)[0].strip("/")
        if tail.startswith("+") or tail.startswith("joinchat/"):
            return None, None
        username = tail.split("/", 1)[0]
    else:
        username = value.lstrip("@")
    if not re.fullmatch(r"[A-Za-z0-9_]{5,32}", username or ""):
        return None, None
    return "@" + username, "https://t.me/" + username

def _channel_label(doc):
    title = (doc.get("title") or doc.get("username") or "Telegram Channel").strip()
    if len(title) > 45:
        title = title[:42] + "..."
    return title

@bot.message_handler(func=lambda m: m.text == "➕ ADD CHANNEL")
def add_promo_channel_start(m):
    if force_block(m.from_user.id):
        return
    _channel_submit_sessions[m.from_user.id] = {"step": "channel"}
    raw_bot.send_message(
        m.from_user.id,
        "➕ ADD YOUR CHANNEL\n\n"
        "1️⃣ Add this bot as an administrator in your channel.\n"
        "2️⃣ Then send the public channel username or link here.\n\n"
        "Examples:\n@mychannel\nhttps://t.me/mychannel\n\n"
        "Your channel will be checked and sent to the admin for approval. It will not appear publicly before approval.\n\n"
        "Send /cancel to stop.",
        reply_markup=main_menu(m.from_user.id),
        disable_web_page_preview=True,
    )

@bot.message_handler(func=lambda m: m.from_user and m.from_user.id in _channel_submit_sessions and _channel_submit_sessions[m.from_user.id].get("step") == "channel", content_types=["text"])
def receive_promo_channel(m):
    uid = m.from_user.id
    if (m.text or "").strip().lower() == "/cancel":
        _channel_submit_sessions.pop(uid, None)
        return raw_bot.send_message(uid, "❌ Channel submission cancelled.", reply_markup=main_menu(uid))
    username, join_url = _normalize_channel_reference(m.text)
    if not username:
        return raw_bot.send_message(uid, "❌ Send a valid public channel username or t.me link.\nExample: @mychannel")
    try:
        chat = bot.get_chat(username)
        if getattr(chat, "type", None) != "channel":
            return raw_bot.send_message(uid, "❌ This is not a Telegram channel. Please send a channel username/link.")
        me = bot.get_me()
        member = bot.get_chat_member(chat.id, me.id)
        if member.status not in ("administrator", "creator"):
            return raw_bot.send_message(uid, "❌ The bot is not an administrator in that channel yet.\n\nMake the bot admin, then send the channel again.")
        owner_member = None
        try:
            owner_member = bot.get_chat_member(chat.id, uid)
        except Exception:
            pass
        if owner_member and owner_member.status not in ("administrator", "creator"):
            return raw_bot.send_message(uid, "❌ You must be an administrator of the submitted channel.")

        existing = promoted_channels_col.find_one({"chat_id": int(chat.id), "status": {"$in": ["pending", "approved"]}})
        if existing:
            _channel_submit_sessions.pop(uid, None)
            status = existing.get("status", "pending")
            return raw_bot.send_message(uid, f"ℹ️ This channel is already {status}.", reply_markup=main_menu(uid))

        doc = {
            "chat_id": int(chat.id),
            "username": "@" + (chat.username or username.lstrip("@")),
            "join_url": "https://t.me/" + (chat.username or username.lstrip("@")),
            "title": getattr(chat, "title", None) or username,
            "submitted_by": str(uid),
            "submitted_by_username": getattr(m.from_user, "username", None),
            "submitted_by_name": " ".join(filter(None, [getattr(m.from_user, "first_name", None), getattr(m.from_user, "last_name", None)])),
            "status": "pending",
            "submitted_at": time.time(),
        }
        result = promoted_channels_col.insert_one(doc)
        _channel_submit_sessions.pop(uid, None)
        raw_bot.send_message(uid, "✅ CHANNEL SUBMITTED\n\nYour channel is pending admin approval. You will be notified after review.", reply_markup=main_menu(uid))
        kb = InlineKeyboardMarkup(row_width=2)
        rid = str(result.inserted_id)
        kb.add(
            InlineKeyboardButton("✅ Approve", callback_data=f"chanapprove|{rid}"),
            InlineKeyboardButton("❌ Reject", callback_data=f"chanreject|{rid}"),
        )
        submitter = "@" + m.from_user.username if m.from_user.username else str(uid)
        for admin in get_all_admins():
            aid = int(admin.get("_id"))
            try:
                raw_bot.send_message(
                    aid,
                    f"📣 CHANNEL APPROVAL REQUEST\n\nChannel: {doc['title']}\nUsername: {doc['username']}\nSubmitted by: {submitter}\nUser ID: {uid}",
                    reply_markup=kb,
                    disable_web_page_preview=True,
                )
            except Exception:
                pass
    except Exception as exc:
        raw_bot.send_message(uid, f"❌ Could not verify that channel.\n\nMake sure the username is correct and the bot is an administrator.\nError: {str(exc)[:300]}")

@bot.message_handler(func=lambda m: m.text == "📢 CHANNELS")
def public_channels_list(m):
    if force_block(m.from_user.id):
        return
    channels = list(promoted_channels_col.find({"status": "approved"}).sort([("approved_at", -1), ("submitted_at", -1)]).limit(100))
    if not channels:
        return raw_bot.send_message(m.from_user.id, "📢 COMMUNITY CHANNELS\n\nNo user channels have been approved yet.", reply_markup=main_menu(m.from_user.id))
    kb = InlineKeyboardMarkup(row_width=1)
    for ch in channels:
        url = ch.get("join_url")
        if url:
            kb.add(InlineKeyboardButton("📢 " + _channel_label(ch), url=url))
    raw_bot.send_message(
        m.from_user.id,
        f"📢 COMMUNITY CHANNELS\n\nExplore {len(channels)} admin-approved channel{'s' if len(channels) != 1 else ''}.\n\nTap a button below to join:",
        reply_markup=kb,
        disable_web_page_preview=True,
    )


# =========================
# 📨 GROUP MESSENGER
# =========================
_group_message_targets = {}


def _group_messenger_picker(admin_id):
    groups = _managed_group_ids()
    if not groups:
        return raw_bot.send_message(admin_id, "❌ No managed groups are configured. Add groups from Group Management first.")
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("📣 Send to ALL managed groups", callback_data="groupmsg|all"))
    for gid in groups:
        try:
            chat = bot.get_chat(gid)
            title = chat.title or str(gid)
        except Exception:
            title = str(gid)
        kb.add(InlineKeyboardButton(f"💬 {title}", callback_data=f"groupmsg|{gid}"))
    raw_bot.send_message(admin_id, "📨 GROUP MESSENGER\n\nChoose one group or send to all managed groups:", reply_markup=kb)


def _group_auto_config():
    doc = config_col.find_one({"_id": "group_auto_message"}) or {}
    return {
        "_id": "group_auto_message",
        "active": bool(doc.get("active", False)),
        "targets": doc.get("targets", []),
        "source_chat": doc.get("source_chat"),
        "source_message": doc.get("source_message"),
        "interval_minutes": int(doc.get("interval_minutes", 60) or 60),
        "next_run": doc.get("next_run"),
        "last_run": doc.get("last_run"),
        "updated_at": doc.get("updated_at"),
    }


def _save_group_auto(**updates):
    updates["updated_at"] = now_ts()
    config_col.update_one({"_id": "group_auto_message"}, {"$set": updates}, upsert=True)


def _group_auto_status_text():
    cfg = _group_auto_config()
    target_count = len(cfg.get("targets") or [])
    interval = cfg.get("interval_minutes", 60)
    hours, minutes = divmod(interval, 60)
    interval_text = []
    if hours:
        interval_text.append(f"{hours} hour{'s' if hours != 1 else ''}")
    if minutes:
        interval_text.append(f"{minutes} minute{'s' if minutes != 1 else ''}")
    if not interval_text:
        interval_text = ["not set"]
    next_text = "Not scheduled"
    if cfg.get("next_run"):
        try:
            next_text = datetime.fromtimestamp(cfg["next_run"]).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            pass
    return (
        "🤖 AUTO GROUP MESSAGE\n\n"
        f"Status: {'🟢 ON' if cfg.get('active') else '🔴 OFF'}\n"
        f"Message: {'✅ Saved' if cfg.get('source_message') else '❌ Not set'}\n"
        f"Groups: {target_count}\n"
        f"Interval: {' '.join(interval_text)}\n"
        f"Next send: {next_text}"
    )


def _group_messenger_menu_kb():
    cfg = _group_auto_config()
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("📨 Send Now", callback_data="groupmsgmenu|sendnow"),
        InlineKeyboardButton("✏️ Set Auto Message", callback_data="groupmsgmenu|setmsg"),
    )
    kb.add(
        InlineKeyboardButton("👥 Select Groups", callback_data="groupmsgmenu|targets"),
        InlineKeyboardButton("⏱ Set Interval", callback_data="groupmsgmenu|interval"),
    )
    kb.add(
        InlineKeyboardButton("🟢 Turn ON" if not cfg.get("active") else "🔴 Turn OFF", callback_data="groupmsgmenu|toggle"),
        InlineKeyboardButton("🧪 Send Test", callback_data="groupmsgmenu|test"),
    )
    kb.add(InlineKeyboardButton("🔄 Refresh", callback_data="groupmsgmenu|refresh"))
    return kb


@bot.message_handler(func=lambda m: m.text == "📨 Group Messenger" and is_admin(m.from_user.id))
def group_messenger_menu(m):
    raw_bot.send_message(m.from_user.id, _group_auto_status_text(), reply_markup=_group_messenger_menu_kb())


@bot.callback_query_handler(func=lambda c: c.data.startswith("groupmsgmenu|"))
def group_messenger_menu_cb(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admins only", True)
    action = c.data.split("|", 1)[1]
    try:
        if action == "sendnow":
            bot.answer_callback_query(c.id)
            return _group_messenger_picker(c.from_user.id)
        if action == "setmsg":
            msg = raw_bot.send_message(c.from_user.id, "✏️ Send or forward the message that should be posted automatically to groups.\n\nText, photo, video, document, audio, animation and voice are supported.\nSend /cancel to stop.")
            bot.register_next_step_handler(msg, save_group_auto_message)
            return bot.answer_callback_query(c.id, "Send the auto message")
        if action == "targets":
            groups = _managed_group_ids()
            if not groups:
                return bot.answer_callback_query(c.id, "No managed groups configured", True)
            kb = InlineKeyboardMarkup(row_width=1)
            kb.add(InlineKeyboardButton("📣 ALL managed groups", callback_data="groupautotarget|all"))
            for gid in groups:
                try:
                    title = bot.get_chat(gid).title or str(gid)
                except Exception:
                    title = str(gid)
                kb.add(InlineKeyboardButton(f"💬 {title}", callback_data=f"groupautotarget|{gid}"))
            raw_bot.send_message(c.from_user.id, "👥 Choose the group for automatic messages, or select all groups:", reply_markup=kb)
            return bot.answer_callback_query(c.id)
        if action == "interval":
            msg = raw_bot.send_message(c.from_user.id, "⏱ Enter how often the automatic message should be sent.\n\nExamples:\n30m\n2h\n1d\n90 (minutes)\n\nMinimum: 5 minutes")
            bot.register_next_step_handler(msg, save_group_auto_interval)
            return bot.answer_callback_query(c.id, "Enter the interval")
        if action == "toggle":
            cfg = _group_auto_config()
            new_state = not cfg.get("active")
            if new_state and (not cfg.get("source_message") or not cfg.get("targets")):
                return bot.answer_callback_query(c.id, "Set the message and groups first", True)
            updates = {"active": new_state}
            if new_state:
                updates["next_run"] = now_ts() + max(cfg.get("interval_minutes", 60), 5) * 60
            _save_group_auto(**updates)
            bot.answer_callback_query(c.id, "Auto messages enabled" if new_state else "Auto messages disabled", True)
        elif action == "test":
            cfg = _group_auto_config()
            if not cfg.get("source_message") or not cfg.get("targets"):
                return bot.answer_callback_query(c.id, "Set the message and groups first", True)
            sent, failed = _send_group_auto_message(cfg)
            bot.answer_callback_query(c.id, f"Sent to {sent} group(s)" if sent else "Test failed", True)
            if failed:
                raw_bot.send_message(c.from_user.id, "⚠️ Test failures:\n" + "\n".join(failed[:10]))
        elif action == "refresh":
            bot.answer_callback_query(c.id)
        try:
            bot.edit_message_text(_group_auto_status_text(), c.from_user.id, c.message.message_id, reply_markup=_group_messenger_menu_kb())
        except Exception:
            raw_bot.send_message(c.from_user.id, _group_auto_status_text(), reply_markup=_group_messenger_menu_kb())
    except Exception as exc:
        admin_error(c.from_user.id, exc)


def save_group_auto_message(m):
    if not is_admin(m.from_user.id):
        return
    if (m.text or "").strip().lower() == "/cancel":
        return raw_bot.send_message(m.from_user.id, "❌ Auto-message setup cancelled.", reply_markup=admin_menu())
    _save_group_auto(source_chat=m.chat.id, source_message=m.message_id, active=False)
    raw_bot.send_message(m.from_user.id, "✅ PROCESS COMPLETE\n\nThe automatic group message has been saved. Select groups and interval, then turn it ON.", reply_markup=_group_messenger_menu_kb())


def _parse_interval_minutes(value):
    text = (value or "").strip().lower().replace(" ", "")
    if not text:
        raise ValueError("Interval is required")
    multiplier = 1
    if text.endswith("m"):
        text = text[:-1]
    elif text.endswith("h"):
        multiplier = 60
        text = text[:-1]
    elif text.endswith("d"):
        multiplier = 1440
        text = text[:-1]
    amount = float(text)
    minutes = int(amount * multiplier)
    if minutes < 5:
        raise ValueError("Minimum interval is 5 minutes")
    if minutes > 525600:
        raise ValueError("Maximum interval is 365 days")
    return minutes


def save_group_auto_interval(m):
    if not is_admin(m.from_user.id):
        return
    try:
        minutes = _parse_interval_minutes(m.text)
        _save_group_auto(interval_minutes=minutes, next_run=now_ts() + minutes * 60, active=False)
        raw_bot.send_message(m.from_user.id, f"✅ PROCESS COMPLETE\n\nAutomatic group message interval set to {minutes} minute{'s' if minutes != 1 else ''}.\nTurn it ON when ready.", reply_markup=_group_messenger_menu_kb())
    except Exception as exc:
        raw_bot.send_message(m.from_user.id, f"❌ PROCESS FAILED\n\n{exc}", reply_markup=_group_messenger_menu_kb())


@bot.callback_query_handler(func=lambda c: c.data.startswith("groupautotarget|"))
def group_auto_target_cb(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admins only", True)
    target = c.data.split("|", 1)[1]
    if target == "all":
        targets = _managed_group_ids()
    else:
        try:
            targets = [int(target)]
        except Exception:
            return bot.answer_callback_query(c.id, "Invalid group", True)
    if not targets:
        return bot.answer_callback_query(c.id, "No groups configured", True)
    _save_group_auto(targets=targets, active=False)
    bot.answer_callback_query(c.id, f"Selected {len(targets)} group(s)", True)
    raw_bot.send_message(c.from_user.id, "✅ PROCESS COMPLETE\n\nAutomatic message destination updated.", reply_markup=_group_messenger_menu_kb())


def _send_group_auto_message(cfg):
    sent = 0
    failed = []
    for gid in cfg.get("targets") or []:
        try:
            raw_bot.copy_message(int(gid), int(cfg["source_chat"]), int(cfg["source_message"]))
            sent += 1
        except Exception as exc:
            failed.append(f"{gid}: {str(exc)[:120]}")
    return sent, failed


@bot.callback_query_handler(func=lambda c: c.data.startswith("groupmsg|"))
def group_messenger_target_cb(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admins only", True)
    target = c.data.split("|", 1)[1]
    if target == "all":
        targets = _managed_group_ids()
    else:
        try:
            targets = [int(target)]
        except Exception:
            return bot.answer_callback_query(c.id, "Invalid group", True)
    if not targets:
        return bot.answer_callback_query(c.id, "No groups configured", True)
    _group_message_targets[c.from_user.id] = targets
    msg = raw_bot.send_message(
        c.from_user.id,
        f"📨 Send or forward the message now.\n\nIt will be delivered to {len(targets)} group{'s' if len(targets) != 1 else ''}.\nYou may send text, photo, video, document, audio, animation, voice, or a forwarded post.\n\nSend /cancel to stop."
    )
    bot.register_next_step_handler(msg, deliver_group_message)
    bot.answer_callback_query(c.id, "Send the message now")


def deliver_group_message(m):
    if not is_admin(m.from_user.id):
        return
    targets = _group_message_targets.pop(m.from_user.id, None)
    if not targets:
        return raw_bot.send_message(m.from_user.id, "❌ Session expired. Open Group Messenger again.")
    if (m.text or "").strip().lower() == "/cancel":
        return raw_bot.send_message(m.from_user.id, "❌ Group message cancelled.", reply_markup=admin_menu())
    sent = 0
    failed = []
    for gid in targets:
        try:
            raw_bot.copy_message(gid, m.chat.id, m.message_id)
            sent += 1
        except Exception as exc:
            failed.append(f"{gid}: {str(exc)[:120]}")
    if sent:
        text = f"✅ PROCESS COMPLETE\n\nMessage delivered to {sent}/{len(targets)} group{'s' if len(targets) != 1 else ''}."
        if failed:
            text += "\n\n⚠️ Failed:\n" + "\n".join(failed[:10])
        raw_bot.send_message(m.from_user.id, text, reply_markup=admin_menu())
    else:
        raw_bot.send_message(m.from_user.id, "❌ PROCESS FAILED\n\nCould not deliver the message.\n" + "\n".join(failed[:10]), reply_markup=admin_menu())


_channel_message_targets = {}
_channel_message_drafts = {}

def _approved_channel_picker(admin_id):
    docs = list(promoted_channels_col.find({"status": "approved"}).sort("approved_at", -1).limit(100))
    if not docs:
        return raw_bot.send_message(admin_id, "❌ No approved channels are available.")
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("📣 Send to ALL approved channels", callback_data="chanmsg|all"))
    for doc in docs:
        kb.add(InlineKeyboardButton("📢 " + _channel_label(doc), callback_data=f"chanmsg|{doc['_id']}"))
    raw_bot.send_message(admin_id, "📨 CHANNEL MESSENGER\n\nChoose one approved channel or send to all approved channels:", reply_markup=kb)


@bot.message_handler(func=lambda m: m.text == "📣 Channel Approvals" and is_admin(m.from_user.id))
def channel_approvals_menu(m):
    pending = list(promoted_channels_col.find({"status": "pending"}).sort("submitted_at", 1).limit(50))
    kb = InlineKeyboardMarkup(row_width=1)
    for ch in pending:
        kb.add(InlineKeyboardButton(f"⏳ {_channel_label(ch)}", callback_data=f"chanreview|{ch['_id']}"))
    kb.add(InlineKeyboardButton("📋 Approved Channels", callback_data="chanapprovedlist"))
    kb.add(InlineKeyboardButton("📨 Channel Messenger", callback_data="chanmessenger"))
    raw_bot.send_message(m.from_user.id, f"📣 CHANNEL APPROVALS\n\nPending: {len(pending)}", reply_markup=kb)

@bot.callback_query_handler(func=lambda c: c.data.startswith("chanreview|"))
def channel_review_cb(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admins only", True)
    from bson import ObjectId
    try:
        doc = promoted_channels_col.find_one({"_id": ObjectId(c.data.split("|", 1)[1])})
    except Exception:
        doc = None
    if not doc:
        return bot.answer_callback_query(c.id, "Channel record not found", True)
    kb = InlineKeyboardMarkup(row_width=2)
    rid = str(doc["_id"])
    if doc.get("status") == "pending":
        kb.add(InlineKeyboardButton("✅ Approve", callback_data=f"chanapprove|{rid}"), InlineKeyboardButton("❌ Reject", callback_data=f"chanreject|{rid}"))
    elif doc.get("status") == "approved":
        kb.add(
            InlineKeyboardButton("📨 Send Message", callback_data=f"chanmsg|{rid}"),
            InlineKeyboardButton("🗑 Remove", callback_data=f"chanremove|{rid}"),
        )
        kb.add(InlineKeyboardButton("❌ Delete Permanently", callback_data=f"chandeleteask|{rid}"))
    if doc.get("join_url"):
        kb.add(InlineKeyboardButton("🔗 Open Channel", url=doc["join_url"]))
    raw_bot.send_message(c.from_user.id, f"📣 CHANNEL REVIEW\n\nTitle: {doc.get('title')}\nUsername: {doc.get('username')}\nStatus: {doc.get('status')}\nSubmitted by: {doc.get('submitted_by_username') or doc.get('submitted_by')}", reply_markup=kb)
    bot.answer_callback_query(c.id)


@bot.callback_query_handler(func=lambda c: c.data == "chanmessenger")
def channel_messenger_open_cb(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admins only", True)
    bot.answer_callback_query(c.id)
    _approved_channel_picker(c.from_user.id)


@bot.callback_query_handler(func=lambda c: c.data.startswith("chanmsg|"))
def channel_message_target_cb(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admins only", True)
    from bson import ObjectId
    target = c.data.split("|", 1)[1]
    if target == "all":
        docs = list(promoted_channels_col.find({"status": "approved"}, {"chat_id": 1, "title": 1}))
    else:
        try:
            doc = promoted_channels_col.find_one({"_id": ObjectId(target), "status": "approved"})
        except Exception:
            doc = None
        docs = [doc] if doc else []
    targets = [int(d["chat_id"]) for d in docs if d and d.get("chat_id") is not None]
    if not targets:
        return bot.answer_callback_query(c.id, "No approved channel found", True)
    _channel_message_targets[c.from_user.id] = targets
    _channel_message_drafts.pop(c.from_user.id, None)
    msg = raw_bot.send_message(
        c.from_user.id,
        f"📨 CHANNEL MESSAGE\n\nSend or forward the message now. It will be delivered to {len(targets)} channel{'s' if len(targets) != 1 else ''}.\n\nYou can send text, photo, video, document, audio, voice, animation, or a forwarded post.\n\nSend /cancel to stop."
    )
    bot.register_next_step_handler(msg, receive_channel_message_content)
    bot.answer_callback_query(c.id, "Send the message now")


def receive_channel_message_content(m):
    if not is_admin(m.from_user.id):
        return
    targets = _channel_message_targets.get(m.from_user.id)
    if not targets:
        return raw_bot.send_message(m.from_user.id, "❌ Session expired. Open Channel Messenger again.")
    if (m.text or "").strip().lower() == "/cancel":
        _channel_message_targets.pop(m.from_user.id, None)
        _channel_message_drafts.pop(m.from_user.id, None)
        return raw_bot.send_message(m.from_user.id, "❌ Channel message cancelled.", reply_markup=admin_menu())
    _channel_message_drafts[m.from_user.id] = {
        "source_chat": m.chat.id,
        "source_message": m.message_id,
    }
    msg = raw_bot.send_message(
        m.from_user.id,
        "🔘 ADD BUTTONS\n\nSend buttons using one button per line:\n\nButton Name | https://example.com\nSecond Button | https://t.me/username\n\nSend `skip` to send without buttons, or /cancel to stop.",
        parse_mode=None,
        disable_web_page_preview=True,
    )
    bot.register_next_step_handler(msg, receive_channel_message_buttons)


def _parse_channel_buttons(text):
    value = (text or "").strip()
    if not value or value.lower() == "skip":
        return None, None
    kb = InlineKeyboardMarkup(row_width=1)
    count = 0
    for raw_line in value.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if "|" not in line:
            return None, "Use: Button Name | https://example.com"
        label, url = [x.strip() for x in line.split("|", 1)]
        if not label or not re.match(r"^https?://", url, re.I):
            return None, f"Invalid button line: {line[:80]}"
        kb.add(InlineKeyboardButton(label[:64], url=url))
        count += 1
        if count >= 8:
            break
    if count == 0:
        return None, "No valid buttons found."
    return kb, None


def receive_channel_message_buttons(m):
    uid = m.from_user.id
    if not is_admin(uid):
        return
    targets = _channel_message_targets.pop(uid, None)
    draft = _channel_message_drafts.pop(uid, None)
    if not targets or not draft:
        return raw_bot.send_message(uid, "❌ Session expired. Open Channel Messenger again.")
    if (m.text or "").strip().lower() == "/cancel":
        return raw_bot.send_message(uid, "❌ Channel message cancelled.", reply_markup=admin_menu())
    kb, error = _parse_channel_buttons(m.text)
    if error:
        # restore session and ask again
        _channel_message_targets[uid] = targets
        _channel_message_drafts[uid] = draft
        msg = raw_bot.send_message(uid, f"❌ {error}\n\nTry again, send `skip`, or /cancel.", parse_mode=None)
        bot.register_next_step_handler(msg, receive_channel_message_buttons)
        return
    sent = 0
    failed = []
    for chat_id in targets:
        try:
            raw_bot.copy_message(
                int(chat_id),
                int(draft["source_chat"]),
                int(draft["source_message"]),
                reply_markup=kb,
            )
            sent += 1
        except Exception as exc:
            failed.append(f"{chat_id}: {str(exc)[:120]}")
    text = f"✅ PROCESS COMPLETE\n\nMessage delivered to {sent}/{len(targets)} channel{'s' if len(targets) != 1 else ''}."
    if failed:
        text += "\n\n⚠️ Failed:\n" + "\n".join(failed[:10])
    raw_bot.send_message(uid, text, reply_markup=admin_menu())


@bot.callback_query_handler(func=lambda c: c.data.startswith("chandeleteask|"))
def channel_delete_ask_cb(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admins only", True)
    rid = c.data.split("|", 1)[1]
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("✅ Yes, Delete", callback_data=f"chandelete|{rid}"),
        InlineKeyboardButton("↩️ Cancel", callback_data=f"chanreview|{rid}"),
    )
    raw_bot.send_message(c.from_user.id, "⚠️ PERMANENT DELETE\n\nThis removes the channel record completely from the bot and public Channels list. Continue?", reply_markup=kb)
    bot.answer_callback_query(c.id)


@bot.callback_query_handler(func=lambda c: c.data.startswith("chandelete|"))
def channel_delete_confirm_cb(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admins only", True)
    from bson import ObjectId
    try:
        oid = ObjectId(c.data.split("|", 1)[1])
    except Exception:
        return bot.answer_callback_query(c.id, "Invalid record", True)
    doc = promoted_channels_col.find_one({"_id": oid})
    if not doc:
        return bot.answer_callback_query(c.id, "Channel already deleted", True)
    promoted_channels_col.delete_one({"_id": oid})
    try:
        raw_bot.send_message(int(doc.get("submitted_by")), f"🗑 CHANNEL REMOVED\n\n{doc.get('title') or doc.get('username')} was permanently removed from the bot by an admin.", reply_markup=main_menu(int(doc.get("submitted_by"))))
    except Exception:
        pass
    try:
        bot.edit_message_text("✅ Channel permanently deleted from the bot.", c.message.chat.id, c.message.message_id)
    except Exception:
        raw_bot.send_message(c.from_user.id, "✅ Channel permanently deleted from the bot.")
    bot.answer_callback_query(c.id, "Deleted")


def send_approved_channel_promo(channel_doc):
    """Post the bot promotion in a newly approved channel."""
    chat_id = int(channel_doc["chat_id"])
    try:
        bot_username = bot.get_me().username or "zedoxxbot"
    except Exception:
        bot_username = "zedoxxbot"
    bot_url = f"https://t.me/{bot_username}"
    text = (
        "🚀 JOIN ZEDOX VIP BOT\n\n"
        "Discover the best channels, premium methods, accounts, private material, "
        "exclusive updates and useful digital resources — all in one place.\n\n"
        f"🤖 @{bot_username}"
    )
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("🚀 Open ZEDOX VIP Bot", url=bot_url))
    message = raw_bot.send_message(chat_id, text, reply_markup=kb, disable_web_page_preview=True)
    promoted_channels_col.update_one(
        {"_id": channel_doc["_id"]},
        {"$set": {"promo_message_id": getattr(message, "message_id", None), "promo_sent_at": time.time()}}
    )
    return message


@bot.callback_query_handler(func=lambda c: c.data.startswith(("chanapprove|", "chanreject|", "chanremove|")))
def channel_decision_cb(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admins only", True)
    from bson import ObjectId
    action, rid = c.data.split("|", 1)
    try:
        oid = ObjectId(rid)
    except Exception:
        return bot.answer_callback_query(c.id, "Invalid record", True)
    doc = promoted_channels_col.find_one({"_id": oid})
    if not doc:
        return bot.answer_callback_query(c.id, "Channel record not found", True)
    now = time.time()
    if action == "chanapprove":
        # Re-check that the bot remains an admin before approval.
        try:
            member = bot.get_chat_member(int(doc["chat_id"]), bot.get_me().id)
            if member.status not in ("administrator", "creator"):
                return bot.answer_callback_query(c.id, "Bot is no longer admin in this channel", True)
        except Exception as exc:
            return bot.answer_callback_query(c.id, f"Verification failed: {str(exc)[:100]}", True)
        promoted_channels_col.update_one({"_id": oid}, {"$set": {"status": "approved", "approved_at": now, "approved_by": str(c.from_user.id)}})
        doc = promoted_channels_col.find_one({"_id": oid}) or doc
        try:
            send_approved_channel_promo(doc)
            promo_note = " Promotional post sent successfully."
        except Exception as exc:
            promo_note = f" Channel approved, but promotional post failed: {str(exc)[:140]}"
        status_text = "approved"
        admin_text = "✅ Channel approved and added to CHANNELS." + promo_note
    elif action == "chanreject":
        promoted_channels_col.update_one({"_id": oid}, {"$set": {"status": "rejected", "reviewed_at": now, "reviewed_by": str(c.from_user.id)}})
        status_text = "rejected"
        admin_text = "❌ Channel rejected."
    else:
        promoted_channels_col.update_one({"_id": oid}, {"$set": {"status": "removed", "removed_at": now, "removed_by": str(c.from_user.id)}})
        status_text = "removed"
        admin_text = "🗑 Channel removed from public list."
    try:
        raw_bot.send_message(int(doc["submitted_by"]), f"📢 CHANNEL REVIEW RESULT\n\n{doc.get('title')} has been {status_text} by the admin.", reply_markup=main_menu(int(doc["submitted_by"])))
    except Exception:
        pass
    try:
        bot.edit_message_text(admin_text, c.message.chat.id, c.message.message_id)
    except Exception:
        raw_bot.send_message(c.from_user.id, admin_text)
    bot.answer_callback_query(c.id, "Process complete")

@bot.callback_query_handler(func=lambda c: c.data == "chanapprovedlist")
def approved_channels_admin_cb(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admins only", True)
    docs = list(promoted_channels_col.find({"status": "approved"}).sort("approved_at", -1).limit(100))
    kb = InlineKeyboardMarkup(row_width=1)
    for doc in docs:
        kb.add(InlineKeyboardButton("✅ " + _channel_label(doc), callback_data=f"chanreview|{doc['_id']}"))
    raw_bot.send_message(c.from_user.id, f"📋 APPROVED CHANNELS\n\nTotal: {len(docs)}", reply_markup=kb)
    bot.answer_callback_query(c.id)


# =========================
# 🧠 FALLBACK
# =========================
@bot.message_handler(func=lambda m: True)
def fallback(m):
    if not validate_request(m):
        return
    
    uid = m.from_user.id
    
    if force_block(uid):
        return
    
    # Check custom buttons
    for btn in get_custom_buttons():
        if m.text == btn["text"]:
            if btn["type"] == "link":
                kb = InlineKeyboardMarkup()
                kb.add(InlineKeyboardButton("🔗 Open", url=btn["data"]))
                bot.send_message(uid, f"🔗 {btn['text']}", reply_markup=kb)
            elif btn["type"] == "folder":
                f = fs.get_by_number(int(btn["data"]))
                if f:
                    fake = type('obj', (object,), {'from_user': m.from_user, 'id': m.message_id, 'data': f"open|{f['cat']}|{f['name']}|"})
                    open_folder(fake)
            return
    
    known = MAIN_MENU_BUTTONS + [
        "⚙️ ADMIN PANEL", "🔎 Search", "📣 Auto Posts", "📥 Auto Import",
        "🧾 Logs", "💾 Backup/Export", "🙈 Hide Button", "👁 Show Button", "📋 METHODS LIST", "🛡 Group Management", "📢 CHANNELS", "➕ ADD CHANNEL", "📣 Channel Approvals"
    ]
    if m.text and m.text not in known:
        bot.send_message(uid, "❌ Use menu buttons", reply_markup=main_menu(uid))

# =========================
# 🚀 RUN BOT
# =========================
def run_bot():
    print("=" * 50, flush=True)
    print("🚀 ZEDOX BOT - RAILWAY READY", flush=True)
    print(f"👑 Owner ID: {ADMIN_ID}", flush=True)
    print("💾 Existing MongoDB data: preserved", flush=True)
    print("=" * 50, flush=True)

    # Remove any old webhook before long polling.
    bot.remove_webhook()
    time.sleep(1)

    me = bot.get_me()
    print(f"✅ Logged in as @{me.username}", flush=True)

    while True:
        try:
            bot.infinity_polling(
                timeout=30,
                long_polling_timeout=30,
                skip_pending=True,
                allowed_updates=["message", "edited_message", "channel_post", "edited_channel_post", "callback_query", "my_chat_member", "chat_member"],
                restart_on_change=False,
            )
        except KeyboardInterrupt:
            raise
        except Exception as exc:
            log_event("polling_restart", details={"error": str(exc), "trace": traceback.format_exc()}, level="error")
            print(f"⚠️ Polling error; restarting: {exc}", flush=True)
            time.sleep(5)

if __name__ == "__main__":
    run_bot()
