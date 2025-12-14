import os
import logging
import asyncio
import random
from datetime import datetime, date, time, timezone
from dotenv import load_dotenv

from telegram import (
    Update, 
    KeyboardButton, 
    ReplyKeyboardMarkup, 
    ReplyKeyboardRemove, 
    BotCommand,
)

from telegram.ext import (
    Application,
    ApplicationBuilder,
    ContextTypes,
    CommandHandler,
    MessageHandler,
    filters,
)

from notion_client import AsyncClient, errors

# ---------------------------------------------------------------------
# ENV & LOGGING
# ---------------------------------------------------------------------

load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
RAW_DB_ID = os.getenv("NOTION_DATABASE_ID")
AGG_DB_ID = os.getenv("NOTION_AGGREGATE_DATABASE_ID")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
PORT = int(os.getenv("PORT", 8000))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------
# CONSTANTS & CACHE
# ---------------------------------------------------------------------

NOTION_VERSION = "2025-09-03"

QUESTION_TYPES = [
    "Learned Things",
    "Grateful Things",
    "Good Things",
    "Interesting Things",
    "General Entry",
]

# Retry/backoff constants
MAX_RETRIES = 5
BASE_DELAY = 1  # seconds

DATA_SOURCE_ID_CACHE: dict[str, str] = {}
DAILY_PAGE_ID_CACHE: dict[str, dict] = {}
DAILY_LOCK = asyncio.Lock()

# Property name mappings (will be determined at runtime)
RAW_TITLE_PROPERTY = None
AGG_TITLE_PROPERTY = None

# Persistent keyboard
PERSISTENT_KEYBOARD = ReplyKeyboardMarkup(
    [[KeyboardButton("Choose Prompt")]],
    resize_keyboard=True,
)

# Keyboard for the 4 question types
QUESTION_KEYBOARD = ReplyKeyboardMarkup(
    [[q] for q in QUESTION_TYPES],
    resize_keyboard=True,
    one_time_keyboard=False,
)

# ---------------------------------------------------------------------
# NOTION CLIENT
# ---------------------------------------------------------------------

notion = AsyncClient(
    auth=NOTION_TOKEN,
    notion_version=NOTION_VERSION,
)

# ---------------------------------------------------------------------
# RETRY / BACKOFF HELPER
# ---------------------------------------------------------------------

async def with_retry_and_backoff(async_func, *args, **kwargs):
    func_name = async_func.__name__
    for retry_count in range(MAX_RETRIES):
        try:
            return await async_func(*args, **kwargs)
        except errors.APIResponseError as e:
            if e.status in [429, 500, 502, 503, 504] and retry_count < MAX_RETRIES - 1:
                delay = BASE_DELAY * (2 ** retry_count)
                jitter = random.uniform(0, delay)
                wait_time = min(delay + jitter, 60)
                logger.warning(
                    f"Notion API transient error ({e.status}) in {func_name}. "
                    f"Retrying in {wait_time:.2f}s (Attempt {retry_count + 1}/{MAX_RETRIES})."
                )
                await asyncio.sleep(wait_time)
            else:
                logger.error(f"Notion API permanent error or max retries reached in {func_name}: {e}")
                raise
        except Exception as e:
            if retry_count < MAX_RETRIES - 1:
                logger.warning(f"Connection error in {func_name}: {e}. Retrying.")
                await asyncio.sleep(BASE_DELAY)
            else:
                logger.error(f"Fatal error in {func_name} after max retries: {e}")
                raise
    raise Exception(f"Failed to execute {func_name} after {MAX_RETRIES} attempts.")

# ---------------------------------------------------------------------
# SCHEMA INITIALIZATION
# ---------------------------------------------------------------------

async def get_data_source_properties(data_source_id: str) -> dict:
    """Fetch properties from a data source and return {prop_name: prop_config}."""
    async def fetch_ds():
        # Using notion.request for lower-level API access to data_sources
        return await notion.request(
            path=f"data_sources/{data_source_id}",
            method="GET",
        )
    
    ds = await with_retry_and_backoff(fetch_ds)
    return ds.get("properties", {})

async def find_title_property(properties: dict) -> str | None:
    """Find the name of the title property in a data source."""
    for prop_name, prop_config in properties.items():
        if prop_config.get("type") == "title":
            return prop_name
    return None

async def add_property_to_data_source(data_source_id: str, property_name: str, property_config: dict):
    """Add a new property to a data source using PATCH."""
    logger.info(f"  → Adding property '{property_name}'")
    
    async def update_ds():
        return await notion.request(
            path=f"data_sources/{data_source_id}",
            method="PATCH",
            body={
                "properties": {
                    property_name: property_config
                }
            }
        )
    
    await with_retry_and_backoff(update_ds)

async def rename_property(data_source_id: str, old_name: str, new_name: str):
    """Rename an existing property."""
    logger.info(f"  → Renaming property '{old_name}' to '{new_name}'")
    
    async def update_ds():
        return await notion.request(
            path=f"data_sources/{data_source_id}",
            method="PATCH",
            body={
                "properties": {
                    old_name: {"name": new_name}
                }
            }
        )
    
    await with_retry_and_backoff(update_ds)

async def ensure_raw_database_schema(raw_ds_id: str):
    """Ensure raw data source has all required properties."""
    global RAW_TITLE_PROPERTY
    
    logger.info("Checking raw database schema...")
    
    existing_props = await get_data_source_properties(raw_ds_id)
    title_prop = await find_title_property(existing_props)
    
    if not title_prop:
        logger.error("=" * 60)
        logger.error("❌ FATAL: Raw database has NO title property!")
        logger.error("This should never happen. Please check your database.")
        logger.error("=" * 60)
        raise Exception("Raw database missing required title property")
    
    logger.info(f"  ✅ Title property found: '{title_prop}'")
    
    # Rename title property if needed
    if title_prop != "Entry ID":
        await rename_property(raw_ds_id, title_prop, "Entry ID")
        RAW_TITLE_PROPERTY = "Entry ID"
        logger.info(f"  ✅ Renamed to 'Entry ID'")
    else:
        RAW_TITLE_PROPERTY = title_prop
    
    # Define other required properties (excluding title)
    required_props = {
        "Journal entry": {
            "type": "rich_text",
            "rich_text": {}
        },
        "Question type": {
            "type": "select",
            "select": {
                "options": [
                    {"name": qtype, "color": "default"} 
                    for qtype in QUESTION_TYPES
                ]
            }
        },
        # ADDED THE NEW 'Created' PROPERTY HERE
        "Created": {
            "type": "created_time",
            "created_time": {} 
        },
    }
    
    # Check which properties are missing
    missing_props = []
    for prop_name in required_props.keys():
        if prop_name not in existing_props:
            missing_props.append(prop_name)
            logger.info(f"  ❌ Property '{prop_name}' missing")
        else:
            logger.info(f"  ✅ Property '{prop_name}' exists")
    
    # Create missing properties
    if missing_props:
        logger.info(f"Creating {len(missing_props)} missing properties...")
        for prop_name in missing_props:
            await add_property_to_data_source(raw_ds_id, prop_name, required_props[prop_name])
            await asyncio.sleep(0.3)
    
    logger.info("Raw database schema ready ✅")

async def ensure_aggregate_database_schema(agg_ds_id: str):
    """Ensure aggregate data source has all required properties."""
    global AGG_TITLE_PROPERTY
    
    logger.info("Checking aggregate database schema...")
    
    existing_props = await get_data_source_properties(agg_ds_id)
    title_prop = await find_title_property(existing_props)
    
    if not title_prop:
        logger.error("=" * 60)
        logger.error("❌ FATAL: Aggregate database has NO title property!")
        logger.error("This should never happen. Please check your database.")
        logger.error("=" * 60)
        raise Exception("Aggregate database missing required title property")
    
    logger.info(f"  ✅ Title property found: '{title_prop}'")
    
    # Rename title property if needed
    if title_prop != "ID":
        await rename_property(agg_ds_id, title_prop, "ID")
        AGG_TITLE_PROPERTY = "ID"
        logger.info(f"  ✅ Renamed to 'ID'")
    else:
        AGG_TITLE_PROPERTY = title_prop
    
    # Define other required properties
    required_props = {
        "Date": {
            "type": "date",
            "date": {}
        },
    }
    
    # Add all question types as rich_text columns
    for qtype in QUESTION_TYPES:
        required_props[qtype] = {
            "type": "rich_text",
            "rich_text": {}
        }
    
    # Check which properties are missing
    missing_props = []
    for prop_name in required_props.keys():
        if prop_name not in existing_props:
            missing_props.append(prop_name)
            logger.info(f"  ❌ Property '{prop_name}' missing")
        else:
            logger.info(f"  ✅ Property '{prop_name}' exists")
    
    # Create missing properties
    if missing_props:
        logger.info(f"Creating {len(missing_props)} missing properties...")
        for prop_name in missing_props:
            await add_property_to_data_source(agg_ds_id, prop_name, required_props[prop_name])
            await asyncio.sleep(0.3)
    
    logger.info("Aggregate database schema ready ✅")

async def initialize_database_schemas():
    """Initialize both databases with required properties."""
    logger.info("=" * 60)
    logger.info("Starting database schema initialization...")
    logger.info("=" * 60)
    
    try:
        # Get data source IDs
        raw_ds_id = await get_data_source_id(RAW_DB_ID)
        agg_ds_id = await get_data_source_id(AGG_DB_ID)
        
        # Initialize schemas
        await ensure_raw_database_schema(raw_ds_id)
        await ensure_aggregate_database_schema(agg_ds_id)
        
        logger.info("=" * 60)
        logger.info("Database schema initialization complete! ✅")
        logger.info("=" * 60)
    except Exception as e:
        logger.error(f"Failed to initialize database schemas: {e}")
        raise

# ---------------------------------------------------------------------
# NOTION HELPERS
# ---------------------------------------------------------------------

async def _get_data_source_id_internal(database_id: str) -> str:
    if database_id in DATA_SOURCE_ID_CACHE:
        return DATA_SOURCE_ID_CACHE[database_id]
    db = await with_retry_and_backoff(lambda: notion.databases.retrieve(database_id=database_id))
    ds_id = db["data_sources"][0]["id"]
    DATA_SOURCE_ID_CACHE[database_id] = ds_id
    return ds_id

async def get_data_source_id(database_id: str) -> str:
    return await _get_data_source_id_internal(database_id)

async def _create_raw_entry_internal(data_source_id: str, text: str, qtype: str):
    # NOTE: The 'Created' property is handled automatically by Notion
    # and does not need to be passed in the properties payload.
    await notion.pages.create(
        parent={"type": "data_source_id", "data_source_id": data_source_id},
        properties={
            RAW_TITLE_PROPERTY: {"title": [{"text": {"content": datetime.now().isoformat()}}]},
            "Journal entry": {"rich_text": [{"text": {"content": text}}]},
            "Question type": {"select": {"name": qtype}},
            # We explicitly *omit* 'Created' here. Notion handles it.
        },
    )

async def create_raw_entry(data_source_id: str, text: str, qtype: str):
    return await with_retry_and_backoff(_create_raw_entry_internal, data_source_id, text, qtype)

# ---------------------------------------------------------------------

async def get_or_create_daily_row(agg_ds_id: str, today: date) -> dict:
    today_str = today.isoformat()
    if today_str in DAILY_PAGE_ID_CACHE:
        return DAILY_PAGE_ID_CACHE[today_str]

    async with DAILY_LOCK:
        if today_str in DAILY_PAGE_ID_CACHE:
            return DAILY_PAGE_ID_CACHE[today_str]

        async def query_daily():
            return await notion.data_sources.query(
                data_source_id=agg_ds_id,
                filter={"property": "Date", "date": {"equals": today_str}},
            )

        query = await with_retry_and_backoff(query_daily)

        if query["results"]:
            page = query["results"][0]
        else:
            async def create_daily():
                return await notion.pages.create(
                    parent={"type": "data_source_id", "data_source_id": agg_ds_id},
                    properties={
                        AGG_TITLE_PROPERTY: {"title": [{"text": {"content": today_str}}]},
                        "Date": {"date": {"start": today_str}},
                    },
                )
            page = await with_retry_and_backoff(create_daily)

        page_id = page["id"]

        async def retrieve_page(pid):
            return await notion.pages.retrieve(page_id=pid)

        page_data = await with_retry_and_backoff(retrieve_page, page_id)

        cache_entry = {"page_id": page_id}
        for qtype in QUESTION_TYPES:
            current_rich_text = page_data["properties"].get(qtype, {}).get("rich_text", [])
            combined_plain_text = "\n".join(rt.get("plain_text", "") for rt in current_rich_text)
            cache_entry[qtype] = combined_plain_text

        DAILY_PAGE_ID_CACHE[today_str] = cache_entry
        return cache_entry

# ---------------------------------------------------------------------

async def _append_to_daily_column_internal(page_id: str, new_content: str, column: str):
    await notion.pages.update(
        page_id=page_id,
        properties={column: {"rich_text": [{"text": {"content": new_content}}]}},
    )

async def append_to_daily_column(daily_page_cache_entry: dict, column: str, text: str):
    page_id = daily_page_cache_entry["page_id"]
    current_content = daily_page_cache_entry.get(column, "")
    entry_text = f"- {text}"
    new_content = current_content + f"\n{entry_text}" if current_content else entry_text
    daily_page_cache_entry[column] = new_content

    try:
        await with_retry_and_backoff(_append_to_daily_column_internal, page_id, new_content, column)
        logger.debug(f"Updated column '{column}' on page {page_id}")
    except Exception as e:
        logger.error(f"Failed to update column '{column}' on page {page_id}: {e}")

# ---------------------------------------------------------------------
# BACKGROUND TASK
# ---------------------------------------------------------------------

async def run_notion_writes(context: ContextTypes.DEFAULT_TYPE, user_id: int, qtype: str, text: str):
    try:
        raw_ds, agg_ds = await asyncio.gather(
            get_data_source_id(RAW_DB_ID),
            get_data_source_id(AGG_DB_ID),
        )
        today = date.today()
        daily_page_cache_entry = await get_or_create_daily_row(agg_ds, today)

        await asyncio.gather(
            create_raw_entry(raw_ds, text, qtype),
            append_to_daily_column(daily_page_cache_entry, qtype, text),
        )
        logger.info(f"Notion writes completed for user {user_id}")
    except Exception as e:
        logger.error(f"Notion write failed for user {user_id}: {e}")

# ---------------------------------------------------------------------
# TELEGRAM HANDLERS
# ---------------------------------------------------------------------

async def receive_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    user_id = update.message.from_user.id

    # If user clicked a question button, set qtype and wait for entry
    if text in QUESTION_TYPES:
        context.user_data["qtype"] = text
        await update.message.reply_text(
            f"Write your entry for *{text}*:",
            parse_mode="Markdown",
            reply_markup=ReplyKeyboardRemove()
        )
        return

    # Otherwise, treat message as an entry
    qtype = context.user_data.pop("qtype", None)

    # Default to General Entry
    if not qtype:
        qtype = "General Entry"

    await update.message.reply_text(
        f"Saved under *{qtype}* ✍️ (Processing in background...)",
        parse_mode="Markdown",
        reply_markup=QUESTION_KEYBOARD
    )

    context.application.create_task(
        run_notion_writes(context, user_id, qtype, text),
        name=f"notion_write_{user_id}_{datetime.now().timestamp()}",
    )

# ---------------------------------------------------------------------

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Tap a question type to start your reflection:",
        reply_markup=QUESTION_KEYBOARD
    )

# ---------------------------------------------------------------------
# JOB QUEUE
# ---------------------------------------------------------------------

async def clear_daily_cache(context: ContextTypes.DEFAULT_TYPE):
    logger.info("Clearing daily cache")
    DAILY_PAGE_ID_CACHE.clear()

# ---------------------------------------------------------------------
# APP BOOTSTRAP
# ---------------------------------------------------------------------

async def post_init(application: Application):
    """Function to run after the bot has initialized."""
    # Set bot commands
    await application.bot.set_my_commands(
        [
            BotCommand("start", "Start or restart the reflection prompt selection"),
        ]
    )
    
    # Initialize database schemas
    await initialize_database_schemas()

def main():
    application = ApplicationBuilder().token(TELEGRAM_TOKEN).post_init(post_init).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, receive_text))

    if application.job_queue:
        application.job_queue.run_daily(
            clear_daily_cache,
            time=time(hour=0, minute=0, second=1, tzinfo=timezone.utc),
            name="daily_cache_cleanup",
        )

    if WEBHOOK_URL:
        application.run_webhook(
            listen="0.0.0.0",
            port=PORT,
            url_path=TELEGRAM_TOKEN,
            webhook_url=f"{WEBHOOK_URL}/{TELEGRAM_TOKEN}",
        )
    else:
        application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
