"""StarSpin Telegram casino bot. Python 3.10+, aiogram 3, SQLite persistence."""
from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import re
import secrets
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiosqlite
from aiogram import BaseMiddleware, Bot, Dispatcher, F
from aiogram.filters import Command, CommandStart
from aiogram.types import (CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup,
                           LabeledPrice, Message, PreCheckoutQuery)
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("starspin")

TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_IDS = {int(x) for x in os.getenv("ADMIN_IDS", "").replace(" ", "").split(",") if x.isdigit()}
DB_PATH = Path(os.getenv("DATABASE_PATH", "starspin.sqlite3"))
CHIPS_PER_STAR = 10
MIN_WITHDRAWAL = 1500
DB: aiosqlite.Connection
DB_LOCK = asyncio.Lock()

GAMES = {
    "slots": "🎰 Автомат", "basket": "🏀 Баскетбол", "dice": "🎲 Кубик",
    "darts": "🎯 Дартс", "blackjack": "🃏 Игра 21", "coin": "🪙 Орёл или решка",
    "roulette": "🎡 Рулетка",
}
GIFT_CATALOG = {
    "teddy": ("🧸 Мишка", 100), "crystal": ("💎 Кристалл", 150),
    "box": ("🎁 Подарок", 75), "cake": ("🍰 Торт", 120),
    "champagne": ("🍾 Шампанское", 250), "rose": ("🌹 Роза", 10),
}


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


async def execute(sql: str, args: tuple = ()) -> aiosqlite.Cursor:
    async with DB_LOCK:
        cur = await DB.execute(sql, args)
        await DB.commit()
        return cur


async def one(sql: str, args: tuple = ()) -> aiosqlite.Row | None:
    async with DB_LOCK:
        cur = await DB.execute(sql, args)
        return await cur.fetchone()


async def many(sql: str, args: tuple = ()) -> list[aiosqlite.Row]:
    async with DB_LOCK:
        cur = await DB.execute(sql, args)
        return await cur.fetchall()


async def init_db() -> None:
    global DB
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    DB = await aiosqlite.connect(DB_PATH)
    DB.row_factory = aiosqlite.Row
    await DB.executescript("""
    PRAGMA journal_mode=WAL;
    PRAGMA foreign_keys=ON;
    CREATE TABLE IF NOT EXISTS users(
      user_id INTEGER PRIMARY KEY, username TEXT, first_name TEXT NOT NULL,
      balance INTEGER NOT NULL DEFAULT 0, banned INTEGER NOT NULL DEFAULT 0,
      waiting_action TEXT, waiting_data TEXT, active_game TEXT, created_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS ledger(
      id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, amount INTEGER NOT NULL,
      kind TEXT NOT NULL, details TEXT, created_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS payments(
      telegram_charge_id TEXT PRIMARY KEY, user_id INTEGER NOT NULL, stars INTEGER NOT NULL,
      chips INTEGER NOT NULL, created_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS withdrawals(
      id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, amount INTEGER NOT NULL,
      status TEXT NOT NULL DEFAULT 'pending', created_at TEXT NOT NULL, handled_by INTEGER
    );
    CREATE TABLE IF NOT EXISTS gifts(
      id INTEGER PRIMARY KEY AUTOINCREMENT, sender_id INTEGER NOT NULL, recipient_id INTEGER NOT NULL,
      gift_name TEXT NOT NULL, price INTEGER NOT NULL, caption TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS duels(
      id INTEGER PRIMARY KEY AUTOINCREMENT, player1 INTEGER NOT NULL, player2 INTEGER NOT NULL,
      stake INTEGER NOT NULL, status TEXT NOT NULL, turn_id INTEGER, hp1 INTEGER NOT NULL DEFAULT 100,
      hp2 INTEGER NOT NULL DEFAULT 100, shield1 INTEGER NOT NULL DEFAULT 0,
      shield2 INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_users_balance ON users(balance DESC);
    CREATE INDEX IF NOT EXISTS idx_ledger_user ON ledger(user_id, created_at);
    """)
    await DB.commit()
    # Upgrade existing databases created by earlier StarSpin versions.
    cur = await DB.execute("PRAGMA table_info(users)")
    user_columns = {row["name"] for row in await cur.fetchall()}
    if "active_game" not in user_columns:
        await DB.execute("ALTER TABLE users ADD COLUMN active_game TEXT")
    cur = await DB.execute("PRAGMA table_info(gifts)")
    gift_columns = {row["name"] for row in await cur.fetchall()}
    if "caption" not in gift_columns:
        await DB.execute("ALTER TABLE gifts ADD COLUMN caption TEXT NOT NULL DEFAULT ''")
    await DB.commit()


async def register(user: Any) -> None:
    await execute("""INSERT INTO users(user_id,username,first_name,created_at) VALUES(?,?,?,?)
      ON CONFLICT(user_id) DO UPDATE SET username=excluded.username,first_name=excluded.first_name""",
      (user.id, user.username, user.first_name or "Игрок", utcnow()))


async def user_row(user_id: int) -> aiosqlite.Row | None:
    return await one("SELECT * FROM users WHERE user_id=?", (user_id,))


async def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


async def allowed(user_id: int) -> bool:
    row = await user_row(user_id)
    return bool(row and (not row["banned"] or await is_admin(user_id)))


class BanMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        user = getattr(event, "from_user", None)
        if user:
            row = await user_row(user.id)
            if row and row["banned"] and user.id not in ADMIN_IDS:
                if isinstance(event, CallbackQuery):
                    await event.answer("Доступ ограничен.", show_alert=True)
                elif isinstance(event, Message):
                    await event.answer("Доступ к боту ограничен.")
                return None
        return await handler(event, data)


async def change_balance(user_id: int, delta: int, kind: str, details: str = "") -> bool:
    """Atomic credit/debit; debits cannot take a balance below zero."""
    async with DB_LOCK:
        if delta < 0:
            cur = await DB.execute("UPDATE users SET balance=balance+? WHERE user_id=? AND balance>=? AND banned=0",
                                   (delta, user_id, -delta))
        else:
            cur = await DB.execute("UPDATE users SET balance=balance+? WHERE user_id=? AND banned=0", (delta, user_id))
        if cur.rowcount:
            await DB.execute("INSERT INTO ledger(user_id,amount,kind,details,created_at) VALUES(?,?,?,?,?)",
                             (user_id, delta, kind, details, utcnow()))
        await DB.commit()
        return cur.rowcount == 1


def kb(rows: list[list[tuple[str, str]]]) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=label, callback_data=data) for label, data in row] for row in rows
    ])


def main_kb(admin: bool = False) -> InlineKeyboardMarkup:
    rows = [
        [("🎮 Игры", "games"), ("⭐ Пополнить", "deposit")],
        [("👤 Баланс", "balance"), ("🏆 Лидеры", "leaders")],
        [("🎁 Подарить", "gift"), ("💸 Вывести", "withdraw")],
        [("⚔️ Поединок", "duel")],
    ]
    if admin:
        rows.append([("🛠 Админ-панель", "admin:home")])
    return kb(rows)


def game_kb() -> InlineKeyboardMarkup:
    keys = [[("🎰 Автомат", "play:slots"), ("🏀 Баскетбол", "play:basket")],
            [("🎲 Кубик", "play:dice"), ("🎯 Дартс", "play:darts")],
            [("🃏 Игра 21", "play:blackjack"), ("🪙 Монетка", "play:coin")],
            [("🎡 Рулетка", "play:roulette")], [("⬅️ В меню", "home")]]
    return kb(keys)


def game_session_kb() -> InlineKeyboardMarkup:
    return kb([[('⬅️ Назад', 'gameback')]])


async def ask_next_bet(message: Message, user_id: int, game: str) -> None:
    await execute("UPDATE users SET active_game=?,waiting_action=?,waiting_data=NULL WHERE user_id=?",
                  (game,f"bet:{game}",user_id))
    await message.answer(f"Следующий раунд: {GAMES[game]}. Введи новую ставку или нажми «Назад».",reply_markup=game_session_kb())


async def show_home(message: Message) -> None:
    await message.answer("🌟 <b>StarSpin</b>\nКазино-игры за фишки Telegram Stars.\nКурс: 1 ⭐ = 10 фишек.",
                         reply_markup=main_kb(await is_admin(message.from_user.id)), parse_mode="HTML")


async def notify_duel(bot: Bot, duel_id: int, text: str) -> None:
    d = await one("SELECT * FROM duels WHERE id=?", (duel_id,))
    if not d:
        return
    for uid in (d["player1"], d["player2"]):
        try:
            await bot.send_message(uid, text, reply_markup=duel_turn_kb(duel_id) if d["status"] == "active" and d["turn_id"] == uid else None)
        except Exception:
            pass


def duel_turn_kb(duel_id: int) -> InlineKeyboardMarkup:
    return kb([[("⚔️ Атаковать", f"duelmove:{duel_id}:attack"), ("🛡 Защищаться", f"duelmove:{duel_id}:defend")]])


async def settle_duel(bot: Bot, duel_id: int, action: str, user_id: int) -> str:
    async with DB_LOCK:
        cur = await DB.execute("SELECT * FROM duels WHERE id=?", (duel_id,))
        d = await cur.fetchone()
        if not d or d["status"] != "active" or d["turn_id"] != user_id:
            return "Сейчас не твой ход или этот бой уже закончен."
        p1, p2 = d["player1"], d["player2"]
        if action == "defend":
            col = "shield1" if user_id == p1 else "shield2"
            await DB.execute(f"UPDATE duels SET {col}=1 WHERE id=?", (duel_id,))
            event = "🛡 Игрок занял оборону."
        else:
            opponent = p2 if user_id == p1 else p1
            other_shield = d["shield2"] if opponent == p2 else d["shield1"]
            damage = random.randint(12, 30)
            if other_shield:
                damage = max(4, damage - 12)
            hp_col = "hp2" if opponent == p2 else "hp1"
            shield_col = "shield2" if opponent == p2 else "shield1"
            await DB.execute(f"UPDATE duels SET {hp_col}=MAX(0,{hp_col}-?),{shield_col}=0 WHERE id=?", (damage, duel_id))
            event = f"⚔️ Удар нанёс {damage} урона."
        cur = await DB.execute("SELECT * FROM duels WHERE id=?", (duel_id,))
        d = await cur.fetchone()
        if d["hp1"] == 0 or d["hp2"] == 0:
            winner = p1 if d["hp2"] == 0 else p2
            await DB.execute("UPDATE duels SET status='finished',turn_id=NULL WHERE id=?", (duel_id,))
            await DB.execute("UPDATE users SET balance=balance+? WHERE user_id=?", (d["stake"] * 2, winner))
            await DB.execute("INSERT INTO ledger(user_id,amount,kind,details,created_at) VALUES(?,?,?,?,?)",
                             (winner, d["stake"] * 2, "duel_win", f"Поединок #{duel_id}", utcnow()))
            await DB.commit()
            name = (await user_row(winner))["first_name"]
            return f"{event}\n🏁 Поединок окончен! Победитель: {name}. Получено {d['stake']*2} фишек."
        next_uid = p2 if user_id == p1 else p1
        await DB.execute("UPDATE duels SET turn_id=? WHERE id=?", (next_uid, duel_id))
        await DB.commit()
        return f"{event}\n❤️ Здоровье: {d['hp1']} — {d['hp2']}. Теперь ход игрока {next_uid}."


async def handle_wager(message: Message, game: str, stake: int) -> None:
    uid = message.from_user.id
    if stake < 1 or stake > 1_000_000:
        await message.answer("Ставка должна быть от 1 до 1 000 000 фишек.")
        return
    if not await change_balance(uid, -stake, "wager", game):
        await message.answer("Не хватает фишек. Пополни баланс или выбери меньшую ставку.")
        return
    await execute("UPDATE users SET waiting_action=NULL,waiting_data=NULL,active_game=? WHERE user_id=?",(game,uid))
    if game == "blackjack":
        deck = [2, 3, 4, 5, 6, 7, 8, 9, 10, 10, 10, 10, 11] * 4
        random.shuffle(deck)
        player, dealer = [deck.pop(), deck.pop()], [deck.pop(), deck.pop()]
        state = {"stake": stake, "deck": deck, "player": player, "dealer": dealer}
        await execute("UPDATE users SET waiting_action='bj',waiting_data=? WHERE user_id=?", (json.dumps(state), uid))
        await show_blackjack(message, state, reveal=False)
        return
    if game == "slots":
        sent = await message.answer_dice(emoji="🎰")
        value = sent.dice.value
        # Telegram slot values have 64 outcomes; 64 is the jackpot, other match bands pay smaller prizes.
        mult = 25 if value == 64 else (5 if value in (22, 43, 64) else (2 if value in (2, 3, 4, 5, 6, 7, 8, 9, 10, 11) else 0))
    elif game in ("basket", "dice", "darts"):
        emoji = {"basket": "🏀", "dice": "🎲", "darts": "🎯"}[game]
        sent = await message.answer_dice(emoji=emoji)
        value = sent.dice.value
        if game == "basket":
            mult = 2 if value >= 4 else 0
        elif game == "dice":
            mult = 4 if value == 6 else (2 if value == 5 else 0)
        else:
            mult = 4 if value == 6 else (2 if value == 5 else 0)
    elif game == "coin":
        await execute("UPDATE users SET waiting_action='coin_pick',waiting_data=? WHERE user_id=?", (str(stake),uid))
        await message.answer("Выбери сторону монеты:",reply_markup=kb([[("Орёл", "coin:heads"), ("Решка", "coin:tails")], [("⬅️ Назад", "gameback")]]))
        return
    elif game == "roulette":
        await execute("UPDATE users SET waiting_action='roulette_color',waiting_data=? WHERE user_id=?", (str(stake),uid))
        await message.answer("Выбери цвет в европейской рулетке. Выплата за верный цвет — x2:",reply_markup=kb([[("🔴 Красное", "roulette:red"), ("⚫ Чёрное", "roulette:black")], [("⬅️ Назад", "gameback")]]))
        return
    payout = stake * mult
    if payout:
        await change_balance(uid, payout, "game_payout", f"{game} x{mult}")
    await message.answer(f"Итог: {'+' + str(payout-stake) if payout else '-' + str(stake)} фишек. Коэффициент: x{mult}.")
    await ask_next_bet(message,uid,game)


def hand_total(cards: list[int]) -> int:
    total = sum(cards)
    aces = cards.count(11)
    while total > 21 and aces:
        total -= 10
        aces -= 1
    return total


async def show_blackjack(message: Message, state: dict, reveal: bool) -> None:
    p = hand_total(state["player"])
    dealer = hand_total(state["dealer"]) if reveal else "?"
    text = f"🃏 Твои карты: {state['player']} = {p}\nКарты дилера: {state['dealer'] if reveal else str(state['dealer'][0]) + ', ?'} = {dealer}"
    if p >= 21 or reveal:
        await finish_blackjack(message, state, text)
    else:
        await message.answer(text, reply_markup=kb([[("Ещё карту", "bj:hit"), ("Хватит", "bj:stand")], [("⬅️ Назад", "gameback")]]))


async def finish_blackjack(message: Message, state: dict, text: str) -> None:
    p, d = hand_total(state["player"]), hand_total(state["dealer"])
    while d < 17:
        state["dealer"].append(state["deck"].pop())
        d = hand_total(state["dealer"])
    stake = state["stake"]
    if p > 21:
        mult, verdict = 0, "Перебор — ставка проиграла."
    elif d > 21 or p > d:
        mult, verdict = (3 if len(state["player"]) == 2 and p == 21 else 2), "Ты выиграл!"
    elif p == d:
        mult, verdict = 1, "Ничья — ставка возвращена."
    else:
        mult, verdict = 0, "Дилер выиграл."
    await execute("UPDATE users SET waiting_action=NULL,waiting_data=NULL WHERE user_id=?", (message.from_user.id,))
    if mult:
        await change_balance(message.from_user.id, stake * mult, "blackjack_payout", verdict)
    await message.answer(f"{text}\nФинал: у тебя {p}, у дилера {d}. {verdict}\nВозврат: {stake*mult} фишек.")
    await ask_next_bet(message,message.from_user.id,"blackjack")


async def set_waiting(user_id: int, action: str, prompt: str, target: Message | CallbackQuery) -> None:
    current = await user_row(user_id)
    if current and current["waiting_action"]:
        notice = "Сначала заверши текущую операцию или отправь /cancel."
        if isinstance(target, CallbackQuery):
            await target.message.answer(notice)
        else:
            await target.answer(notice)
        return
    await execute("UPDATE users SET waiting_action=?,waiting_data=NULL WHERE user_id=?", (action, user_id))
    if isinstance(target, CallbackQuery):
        await target.message.answer(prompt)
    else:
        await target.answer(prompt)


async def clear_waiting(uid: int) -> None:
    await execute("UPDATE users SET waiting_action=NULL,waiting_data=NULL WHERE user_id=?", (uid,))


async def deliver_gift(bot: Bot, sender_id: int, sender_name: str, info: dict, caption: str) -> bool:
    recipient_id=int(info["recipient_id"]); price=int(info["price"]); gift_name=info["name"]
    async with DB_LOCK:
        cur=await DB.execute("UPDATE users SET balance=balance-? WHERE user_id=? AND balance>=? AND banned=0",(price,sender_id,price))
        if not cur.rowcount:
            await DB.commit(); return False
        await DB.execute("INSERT INTO ledger(user_id,amount,kind,details,created_at) VALUES(?,?,?,?,?)",
                         (sender_id,-price,"gift_sent",f"{gift_name} to {recipient_id}",utcnow()))
        await DB.execute("INSERT INTO gifts(sender_id,recipient_id,gift_name,price,caption,created_at) VALUES(?,?,?,?,?,?)",
                         (sender_id,recipient_id,gift_name,price,caption,utcnow()))
        await DB.commit()
    await bot.send_message(sender_id,f"🎁 Подарок «{gift_name}» отправлен за {price} фишек ({price/CHIPS_PER_STAR:g} ⭐)."+(f"\nПодпись: {caption}" if caption else ""))
    try:
        await bot.send_message(recipient_id,f"🎁 {sender_name} отправил(а) тебе: {gift_name}!"+(f"\n\n💌 {caption}" if caption else ""))
    except Exception:
        pass
    return True


async def resolve_username(value: str) -> aiosqlite.Row | None:
    value = value.strip().lstrip("@")
    if value.isdigit():
        return await user_row(int(value))
    return await one("SELECT * FROM users WHERE lower(username)=lower(?)", (value,))


async def main() -> None:
    if not TOKEN:
        raise RuntimeError("Укажи BOT_TOKEN в файле .env (см. .env.example).")
    await init_db()
    bot = Bot(TOKEN)
    dp = Dispatcher()
    dp.message.outer_middleware(BanMiddleware())
    dp.callback_query.outer_middleware(BanMiddleware())

    @dp.message(CommandStart())
    async def start(message: Message):
        await register(message.from_user)
        if not await allowed(message.from_user.id):
            await message.answer("Доступ к боту ограничен.")
            return
        await show_home(message)

    @dp.message(Command("help"))
    async def help_cmd(message: Message):
        await message.answer("Команды: /start — меню, /balance — баланс, /leaders — рейтинг, /cancel — отменить текущую операцию.\nИгры, подарки и вывод доступны через кнопки меню.\n1 ⭐ = 10 фишек. Минимум для заявки на вывод — 1500 фишек.")

    @dp.message(Command("cancel"))
    async def cancel_cmd(message: Message):
        row=await user_row(message.from_user.id)
        if not row or not row["waiting_action"]:
            await message.answer("Нет незавершённой операции."); return
        action=row["waiting_action"]
        refund=0
        if action=="bj" and row["waiting_data"]:
            refund=json.loads(row["waiting_data"])["stake"]
        elif action in ("coin_pick","roulette_color") and row["waiting_data"]:
            refund=int(row["waiting_data"])
        if refund:
            await change_balance(message.from_user.id,refund,"game_cancel_refund",action)
        await clear_waiting(message.from_user.id)
        await execute("UPDATE users SET active_game=NULL WHERE user_id=?",(message.from_user.id,))
        await message.answer("Операция отменена."+(f" Возвращено {refund} фишек." if refund else ""))

    @dp.message(Command("balance"))
    async def balance_cmd(message: Message):
        row = await user_row(message.from_user.id)
        await message.answer(f"Баланс: <b>{row['balance'] if row else 0}</b> фишек", parse_mode="HTML")

    @dp.message(Command("leaders"))
    async def leaders_cmd(message: Message):
        rows = await many("SELECT first_name,username,balance FROM users WHERE banned=0 ORDER BY balance DESC,user_id LIMIT 10")
        text = "🏆 <b>Глобальный рейтинг</b>\n" + "\n".join(f"{i}. {r['first_name']} (@{r['username'] or '—'}) — {r['balance']} фишек" for i,r in enumerate(rows,1))
        await message.answer(text, parse_mode="HTML")

    @dp.message(Command("admin"))
    async def admin_cmd(message: Message):
        if await is_admin(message.from_user.id):
            await message.answer("🛠 Панель администратора", reply_markup=kb([[('Открыть панель', 'admin:home')], [('Заявки на вывод', 'admin:withdrawals')], [('⬅️ Меню', 'home')]]))

    @dp.message(Command("ban"))
    async def ban_cmd(message: Message):
        if not await is_admin(message.from_user.id): return
        parts = (message.text or "").split()
        if len(parts) != 2 or not parts[1].isdigit():
            await message.answer("Формат: /ban user_id"); return
        await execute("UPDATE users SET banned=1,waiting_action=NULL,waiting_data=NULL WHERE user_id=?", (int(parts[1]),))
        await message.answer("Пользователь заблокирован.")

    @dp.message(Command("unban"))
    async def unban_cmd(message: Message):
        if not await is_admin(message.from_user.id): return
        parts = (message.text or "").split()
        if len(parts) != 2 or not parts[1].isdigit():
            await message.answer("Формат: /unban user_id"); return
        await execute("UPDATE users SET banned=0 WHERE user_id=?", (int(parts[1]),))
        await message.answer("Пользователь разблокирован.")

    @dp.message(Command("setbalance"))
    async def setbalance_cmd(message: Message):
        if not await is_admin(message.from_user.id): return
        parts = (message.text or "").split()
        if len(parts) != 3 or not all(x.lstrip("-").isdigit() for x in parts[1:]):
            await message.answer("Формат: /setbalance user_id сумма"); return
        uid, value = int(parts[1]), int(parts[2])
        if value < 0: await message.answer("Баланс не может быть отрицательным."); return
        row = await user_row(uid)
        if not row: await message.answer("Пользователь ещё не запускал бота."); return
        delta = value-row["balance"]
        async with DB_LOCK:
            await DB.execute("UPDATE users SET balance=? WHERE user_id=?", (value,uid))
            await DB.execute("INSERT INTO ledger(user_id,amount,kind,details,created_at) VALUES(?,?,?,?,?)", (uid,delta,"admin_adjust",f"admin {message.from_user.id}",utcnow()))
            await DB.commit()
        await message.answer(f"Баланс установлен: {value} фишек.")

    @dp.pre_checkout_query()
    async def pre_checkout(query: PreCheckoutQuery):
        payload = query.invoice_payload.split(":")
        valid = len(payload) == 2 and payload[0] == "starspin" and payload[1].isdigit() and int(payload[1]) == query.from_user.id and query.currency == "XTR" and query.total_amount > 0
        await query.answer(ok=valid, error_message=None if valid else "Не удалось проверить платёж.")

    @dp.message(F.successful_payment)
    async def successful_payment(message: Message):
        payment = message.successful_payment
        uid = message.from_user.id
        if payment.currency != "XTR" or payment.total_amount <= 0:
            await message.answer("Платёж не распознан. Обратись к администратору."); return
        chips = payment.total_amount * CHIPS_PER_STAR
        async with DB_LOCK:
            try:
                await DB.execute("INSERT INTO payments(telegram_charge_id,user_id,stars,chips,created_at) VALUES(?,?,?,?,?)",
                                 (payment.telegram_payment_charge_id,uid,payment.total_amount,chips,utcnow()))
            except aiosqlite.IntegrityError:
                await DB.rollback(); await message.answer("Этот платёж уже был зачислен."); return
            await DB.execute("UPDATE users SET balance=balance+? WHERE user_id=?", (chips,uid))
            await DB.execute("INSERT INTO ledger(user_id,amount,kind,details,created_at) VALUES(?,?,?,?,?)", (uid,chips,"stars_topup",f"{payment.total_amount} Telegram Stars",utcnow()))
            await DB.commit()
        await message.answer(f"Зачислено {chips} фишек за {payment.total_amount} ⭐.")

    @dp.callback_query(F.data == "home")
    async def home(c: CallbackQuery):
        await c.answer(); await c.message.answer("🌟 StarSpin — меню", reply_markup=main_kb(await is_admin(c.from_user.id)))

    @dp.callback_query(F.data == "games")
    async def games(c: CallbackQuery): await c.answer(); await c.message.answer("Выбери игру и введи ставку:", reply_markup=game_kb())

    @dp.callback_query(F.data.startswith("play:"))
    async def play(c: CallbackQuery):
        await c.answer()
        game = c.data.split(":")[1]
        if not await allowed(c.from_user.id): await c.message.answer("Доступ ограничен."); return
        current=await user_row(c.from_user.id)
        if current and current["waiting_action"]:
            await c.message.answer("Сначала заверши текущую операцию или отправь /cancel."); return
        await execute("UPDATE users SET active_game=?,waiting_action=?,waiting_data=NULL WHERE user_id=?",(game,f"bet:{game}",c.from_user.id))
        await c.message.answer(f"{GAMES[game]}\nВведи ставку в фишках:",reply_markup=game_session_kb())

    @dp.callback_query(F.data == "gameback")
    async def game_back(c: CallbackQuery):
        row=await user_row(c.from_user.id)
        if not row or not row["active_game"]:
            await c.answer("Нет активной игры",show_alert=True); return
        refund=0
        if row["waiting_action"]=="bj" and row["waiting_data"]:
            refund=json.loads(row["waiting_data"])["stake"]
        elif row["waiting_action"] in ("coin_pick","roulette_color") and row["waiting_data"]:
            refund=int(row["waiting_data"])
        if refund:
            await change_balance(c.from_user.id,refund,"game_cancel_refund",row["waiting_action"])
        await execute("UPDATE users SET active_game=NULL,waiting_action=NULL,waiting_data=NULL WHERE user_id=?",(c.from_user.id,))
        await c.answer("Игра завершена")
        await c.message.answer("Возвращаемся в меню."+(f" Ставка {refund} фишек возвращена." if refund else ""),reply_markup=main_kb(await is_admin(c.from_user.id)))

    @dp.callback_query(F.data == "balance")
    async def balance_cb(c: CallbackQuery):
        await c.answer(); row=await user_row(c.from_user.id)
        await c.message.answer(f"Баланс: <b>{row['balance'] if row else 0}</b> фишек\nЭквивалент пополнения: 1 ⭐ = 10 фишек.", parse_mode="HTML")

    @dp.callback_query(F.data == "leaders")
    async def leaders_cb(c: CallbackQuery):
        await c.answer()
        rows=await many("SELECT first_name,username,balance FROM users WHERE banned=0 ORDER BY balance DESC,user_id LIMIT 10")
        text="🏆 <b>Глобальный рейтинг</b>\n"+"\n".join(f"{i}. {r['first_name']} (@{r['username'] or '—'}) — {r['balance']} фишек" for i,r in enumerate(rows,1))
        await c.message.answer(text,parse_mode="HTML")

    @dp.callback_query(F.data == "deposit")
    async def deposit(c: CallbackQuery):
        await c.answer()
        await set_waiting(c.from_user.id,"deposit","Напиши число Telegram Stars:",c)

    @dp.callback_query(F.data == "withdraw")
    async def withdraw(c: CallbackQuery):
        await c.answer(); await set_waiting(c.from_user.id,"withdraw","Введи сумму в фишках (минимум 1500). Создастся заявка администратору.",c)

    @dp.callback_query(F.data == "gift")
    async def gift(c: CallbackQuery):
        await c.answer()
        rows=[[(name,f"giftitem:{key}") for key,(name,_) in list(GIFT_CATALOG.items())[i:i+2]] for i in range(0,len(GIFT_CATALOG),2)]
        rows.append([("✨ Свой подарок", "giftitem:custom")])
        await c.message.answer("🎁 Выбери подарок. У своего подарка можно указать любое название и цену. Курс: 1 ⭐ = 10 фишек.",reply_markup=kb(rows))

    @dp.callback_query(F.data.startswith("giftitem:"))
    async def gift_item(c: CallbackQuery):
        key=c.data.split(":",1)[1]
        current=await user_row(c.from_user.id)
        if current and current["waiting_action"]:
            await c.answer("Сначала заверши текущую операцию или отправь /cancel.",show_alert=True); return
        await c.answer()
        if key=="custom":
            await set_waiting(c.from_user.id,"gift_custom_name","Напиши любое название подарка (до 40 символов):",c); return
        name,price=GIFT_CATALOG[key]
        await execute("UPDATE users SET waiting_action='gift_user',waiting_data=? WHERE user_id=?",
                      (json.dumps({"name":name,"price":price},ensure_ascii=False),c.from_user.id))
        await c.message.answer(f"{name} стоит {price} фишек ({price/CHIPS_PER_STAR:g} ⭐). Введи @username или Telegram ID получателя.")

    @dp.callback_query(F.data == "giftcaption:skip")
    async def gift_caption_skip(c: CallbackQuery):
        row=await user_row(c.from_user.id)
        if not row or row["waiting_action"]!="gift_caption": await c.answer("Подарок уже обработан",show_alert=True); return
        info=json.loads(row["waiting_data"])
        ok=await deliver_gift(c.bot,c.from_user.id,c.from_user.first_name,info,"")
        await clear_waiting(c.from_user.id); await c.answer()
        if not ok: await c.message.answer("Не хватает фишек для подарка.")

    @dp.callback_query(F.data == "duel")
    async def duel(c: CallbackQuery):
        await c.answer(); await set_waiting(c.from_user.id,"duel_user","Введи @username или Telegram ID соперника.",c)

    @dp.callback_query(F.data.startswith("admin:"))
    async def admin_callbacks(c: CallbackQuery):
        if not await is_admin(c.from_user.id): await c.answer("Нет доступа",show_alert=True); return
        await c.answer()
        if c.data == "admin:home":
            await c.message.answer("🛠 <b>Панель администратора</b>\n/ban user_id — заблокировать\n/unban user_id — разблокировать\n/setbalance user_id сумма — установить баланс",parse_mode="HTML",reply_markup=kb([
                [("Заявки на вывод", "admin:withdrawals")],
                [("🔎 Игрок по ID", "admin:player"), ("➕ Начислить", "admin:add")],
                [("➖ Списать", "admin:subtract")]]))
        elif c.data == "admin:withdrawals":
            rows=await many("SELECT w.*,u.first_name,u.username FROM withdrawals w JOIN users u USING(user_id) WHERE w.status='pending' ORDER BY w.created_at LIMIT 20")
            if not rows: await c.message.answer("Ожидающих заявок нет."); return
            for w in rows:
                await c.message.answer(f"Заявка #{w['id']} — {w['first_name']} (@{w['username'] or '—'}, ID {w['user_id']}), {w['amount']} фишек", reply_markup=kb([[("✅ Выполнена",f"wd:approve:{w['id']}"),("↩️ Отклонить",f"wd:reject:{w['id']}")]]))
        elif c.data == "admin:player":
            await set_waiting(c.from_user.id,"admin_player","Введи Telegram ID игрока:",c)
        elif c.data in ("admin:add","admin:subtract"):
            mode="add" if c.data=="admin:add" else "subtract"
            await set_waiting(c.from_user.id,f"admin_{mode}_user",f"Введи Telegram ID игрока для {'начисления' if mode=='add' else 'списания'} фишек:",c)

    @dp.callback_query(F.data.startswith("wd:"))
    async def withdrawal_action(c: CallbackQuery):
        if not await is_admin(c.from_user.id): await c.answer("Нет доступа",show_alert=True); return
        _, action, raw = c.data.split(":"); wid=int(raw)
        async with DB_LOCK:
            cur=await DB.execute("SELECT * FROM withdrawals WHERE id=? AND status='pending'",(wid,)); w=await cur.fetchone()
            if not w: await c.answer("Заявка уже обработана",show_alert=True); return
            status="paid" if action=="approve" else "rejected"
            await DB.execute("UPDATE withdrawals SET status=?,handled_by=? WHERE id=?",(status,c.from_user.id,wid))
            if action=="reject":
                await DB.execute("UPDATE users SET balance=balance+? WHERE user_id=?",(w['amount'],w['user_id']))
                await DB.execute("INSERT INTO ledger(user_id,amount,kind,details,created_at) VALUES(?,?,?,?,?)",(w['user_id'],w['amount'],"withdraw_refund",f"Заявка #{wid} отклонена",utcnow()))
            await DB.commit()
        await c.answer("Готово")
        await c.message.edit_text(f"Заявка #{wid}: {'выполнена' if action=='approve' else 'отклонена, фишки возвращены'}.")
        try: await c.bot.send_message(w['user_id'], f"Заявка на вывод #{wid} {'отмечена выполненной администратором.' if action=='approve' else 'отклонена. Фишки возвращены на баланс.'}")
        except Exception: pass

    @dp.callback_query(F.data.startswith("bj:"))
    async def blackjack_cb(c: CallbackQuery):
        await c.answer(); row=await user_row(c.from_user.id)
        if not row or row["waiting_action"] != "bj" or not row["waiting_data"]: await c.message.answer("Активная игра 21 не найдена."); return
        state=json.loads(row["waiting_data"])
        if c.data=="bj:hit":
            state["player"].append(state["deck"].pop())
            await execute("UPDATE users SET waiting_data=? WHERE user_id=?",(json.dumps(state),c.from_user.id))
            await show_blackjack(c.message,state,reveal=hand_total(state["player"])>=21)
        else: await show_blackjack(c.message,state,reveal=True)

    @dp.callback_query(F.data.startswith("coin:"))
    async def coin_pick(c: CallbackQuery):
        row=await user_row(c.from_user.id)
        if not row or row["waiting_action"]!="coin_pick": await c.answer("Игра уже завершена",show_alert=True); return
        stake=int(row["waiting_data"]); pick=c.data.split(":")[1]
        result=random.choice(("heads","tails")); won=pick==result
        await clear_waiting(c.from_user.id)
        if won: await change_balance(c.from_user.id,stake*2,"game_payout","coin flip x2")
        await c.answer()
        await c.message.answer(f"🪙 Выпало: {'Орёл' if result=='heads' else 'Решка'}. {'Ты выиграл' if won else 'Ставка проиграла'}; {'+'+str(stake) if won else '-'+str(stake)} фишек.")
        await ask_next_bet(c.message,c.from_user.id,"coin")

    @dp.callback_query(F.data.startswith("roulette:"))
    async def roulette_pick(c: CallbackQuery):
        row=await user_row(c.from_user.id)
        if not row or row["waiting_action"]!="roulette_color": await c.answer("Игра уже завершена",show_alert=True); return
        stake=int(row["waiting_data"]); pick=c.data.split(":")[1]
        result=random.randint(0,36)
        red={1,3,5,7,9,12,14,16,18,19,21,23,25,27,30,32,34,36}
        color="green" if result==0 else ("red" if result in red else "black")
        won=pick==color
        await clear_waiting(c.from_user.id)
        if won: await change_balance(c.from_user.id,stake*2,"game_payout","roulette x2")
        await c.answer()
        color_name={"red":"красное","black":"чёрное","green":"зелёное"}[color]
        await c.message.answer(f"🎡 Выпало {result} ({color_name}). {'Ты выиграл' if won else 'Ставка проиграла'}; {'+'+str(stake) if won else '-'+str(stake)} фишек.")
        await ask_next_bet(c.message,c.from_user.id,"roulette")

    @dp.callback_query(F.data.startswith("duelaccept:"))
    async def duel_accept(c: CallbackQuery):
        did=int(c.data.split(":")[1])
        async with DB_LOCK:
            cur=await DB.execute("SELECT * FROM duels WHERE id=? AND status='pending'",(did,)); d=await cur.fetchone()
            if not d or d["player2"]!=c.from_user.id:
                await c.answer("Вызов уже недоступен",show_alert=True); return
            cur=await DB.execute("UPDATE users SET balance=balance-? WHERE user_id=? AND balance>=? AND banned=0",(d["stake"],c.from_user.id,d["stake"]))
            if not cur.rowcount:
                await c.answer("Не хватает фишек",show_alert=True); return
            await DB.execute("INSERT INTO ledger(user_id,amount,kind,details,created_at) VALUES(?,?,?,?,?)",(c.from_user.id,-d["stake"],"duel_stake",f"Поединок #{did}",utcnow()))
            await DB.execute("UPDATE duels SET status='active',turn_id=player1 WHERE id=?",(did,))
            await DB.commit()
        await c.answer("Поединок начался")
        await c.bot.send_message(d["player1"],f"⚔️ Соперник принял вызов #{did}. Ставки: {d['stake']} фишек с каждого. Твой ход.",reply_markup=duel_turn_kb(did))
        await c.message.answer(f"Поединок #{did} начался! Ставки: {d['stake']} фишек с каждого.")

    @dp.callback_query(F.data.startswith("duelreject:"))
    async def duel_reject(c: CallbackQuery):
        did=int(c.data.split(":")[1]); d=await one("SELECT * FROM duels WHERE id=?",(did,))
        if not d or d["status"]!="pending" or d["player2"]!=c.from_user.id: await c.answer("Вызов уже недоступен",show_alert=True); return
        await execute("UPDATE duels SET status='rejected' WHERE id=?",(did,))
        await change_balance(d["player1"],d["stake"],"duel_refund",f"Поединок #{did} отклонён")
        await c.answer("Вызов отклонён"); await c.message.edit_text("Вызов отклонён; ставка возвращена.")

    @dp.callback_query(F.data.startswith("duelmove:"))
    async def duel_move(c: CallbackQuery):
        _, raw, action = c.data.split(":")
        if action not in ("attack","defend"): await c.answer(); return
        text=await settle_duel(c.bot,int(raw),action,c.from_user.id)
        await c.answer("Ход сделан" if "твой ход" not in text else "Не твой ход",show_alert="не твой ход" in text)
        d=await one("SELECT * FROM duels WHERE id=?",(int(raw),))
        if d:
            for uid in (d["player1"],d["player2"]):
                try: await c.bot.send_message(uid,text,reply_markup=duel_turn_kb(int(raw)) if d["status"]=="active" and d["turn_id"]==uid else None)
                except Exception: pass

    @dp.message(F.text)
    async def text_input(message: Message):
        uid=message.from_user.id
        if await user_row(uid) is None: await register(message.from_user)
        row=await user_row(uid)
        if row["banned"] and not await is_admin(uid): await message.answer("Доступ к боту ограничен."); return
        action=row["waiting_action"]
        if not action: return
        text=message.text.strip()
        if action=="admin_player":
            if not text.isdigit(): await message.answer("Введи Telegram ID цифрами."); return
            target_id=int(text); target=await user_row(target_id)
            if not target: await message.answer("Игрок не найден: он ещё не запускал бота."); return
            gifts=await one("SELECT COUNT(*) AS n FROM gifts WHERE sender_id=? OR recipient_id=?",(target_id,target_id))
            ops=await many("SELECT amount,kind,details,created_at FROM ledger WHERE user_id=? ORDER BY id DESC LIMIT 8",(target_id,))
            text_out=(f"👤 Игрок: {target['first_name']}\nID: {target_id}\nUsername: @{target['username'] or '—'}\n"
                      f"Баланс: {target['balance']} фишек\nБан: {'да' if target['banned'] else 'нет'}\n"
                      f"Регистрация: {target['created_at']}\nПодарков отправлено/получено: {gifts['n']}\n\nПоследние операции:")
            text_out+="\n".join(f"{o['created_at']} · {o['kind']} · {o['amount']:+} · {o['details'] or ''}" for o in ops) or "нет"
            await clear_waiting(uid); await message.answer(text_out); return
        if action in ("admin_add_user","admin_subtract_user"):
            if not text.isdigit(): await message.answer("Введи Telegram ID цифрами."); return
            target_id=int(text); target=await user_row(target_id)
            if not target: await message.answer("Игрок не найден: он ещё не запускал бота."); return
            operation="add" if action=="admin_add_user" else "subtract"
            await execute("UPDATE users SET waiting_action=?,waiting_data=? WHERE user_id=?",(f"admin_{operation}_amount",str(target_id),uid))
            await message.answer(f"Игрок {target['first_name']} (ID {target_id}), баланс {target['balance']}. Введи количество фишек:"); return
        if action in ("admin_add_amount","admin_subtract_amount"):
            if not text.isdigit() or not 1<=int(text)<=1_000_000_000: await message.answer("Введи количество от 1 до 1 000 000 000."); return
            target_id=int(row["waiting_data"]); amount=int(text); operation="add" if action=="admin_add_amount" else "subtract"
            async with DB_LOCK:
                if operation=="add":
                    cur=await DB.execute("UPDATE users SET balance=balance+? WHERE user_id=?",(amount,target_id))
                else:
                    cur=await DB.execute("UPDATE users SET balance=balance-? WHERE user_id=? AND balance>=?",(amount,target_id,amount))
                if cur.rowcount:
                    await DB.execute("INSERT INTO ledger(user_id,amount,kind,details,created_at) VALUES(?,?,?,?,?)",
                                     (target_id,amount if operation=="add" else -amount,f"admin_{operation}",f"admin {uid}",utcnow()))
                await DB.commit()
            await clear_waiting(uid)
            if not cur.rowcount: await message.answer("Не получилось списать: баланс игрока меньше указанной суммы."); return
            target=await user_row(target_id)
            await message.answer(f"Готово. {'Начислено' if operation=='add' else 'Списано'} {amount} фишек игроку {target['first_name']}. Новый баланс: {target['balance']}.")
            try: await message.bot.send_message(target_id,f"Администратор {'начислил' if operation=='add' else 'списал'} {amount} фишек. Новый баланс: {target['balance']}.")
            except Exception: pass
            return
        if action.startswith("bet:"):
            if not text.isdigit(): await message.answer("Введи ставку целым числом."); return
            await handle_wager(message,action.split(":")[1],int(text)); return
        if action=="deposit":
            if not text.isdigit() or not 1<=int(text)<=10000: await message.answer("Введи число от 1 до 10 000."); return
            stars=int(text); await clear_waiting(uid)
            await message.answer_invoice(title="Пополнение StarSpin",description=f"{stars} ⭐ = {stars*CHIPS_PER_STAR} фишек",payload=f"starspin:{uid}",currency="XTR",prices=[LabeledPrice(label="Telegram Stars",amount=stars)])
            return
        if action=="withdraw":
            if not text.isdigit() or int(text)<MIN_WITHDRAWAL: await message.answer(f"Минимум для заявки — {MIN_WITHDRAWAL} фишек. Введи сумму целым числом."); return
            amount=int(text)
            if not await change_balance(uid,-amount,"withdraw_hold","Заявка на вывод"): await message.answer("Не хватает фишек."); await clear_waiting(uid); return
            cur=await execute("INSERT INTO withdrawals(user_id,amount,created_at) VALUES(?,?,?)",(uid,amount,utcnow()))
            await clear_waiting(uid); await message.answer(f"Заявка #{cur.lastrowid} на {amount} фишек создана. Её обработает администратор.")
            for aid in ADMIN_IDS:
                try: await message.bot.send_message(aid,f"Новая заявка на вывод #{cur.lastrowid}: {amount} фишек от {uid}. /admin")
                except Exception: pass
            return
        if action=="coin_pick" or action=="roulette_color":
            await message.answer("Выбери результат кнопкой под сообщением.")
            return
        if action=="gift_custom_name":
            name=text[:40].strip()
            if not name: await message.answer("Название подарка не может быть пустым."); return
            await execute("UPDATE users SET waiting_action='gift_custom_price',waiting_data=? WHERE user_id=?",
                          (json.dumps({"name":name},ensure_ascii=False),uid))
            await message.answer(f"Подарок «{name}». Теперь введи его цену в фишках (1 ⭐ = 10 фишек):")
            return
        if action=="gift_custom_price":
            if not text.isdigit() or not 1<=int(text)<=1_000_000_000: await message.answer("Укажи цену от 1 до 1 000 000 000 фишек."); return
            info=json.loads(row["waiting_data"]); info["price"]=int(text)
            await execute("UPDATE users SET waiting_action='gift_user',waiting_data=? WHERE user_id=?",
                          (json.dumps(info,ensure_ascii=False),uid))
            await message.answer(f"Подарок «{info['name']}» стоит {int(text)} фишек ({int(text)/CHIPS_PER_STAR:g} ⭐). Введи @username или Telegram ID получателя:")
            return
        if action=="gift_user":
            recipient=await resolve_username(text)
            if not recipient: await message.answer("Не нашёл активного пользователя. Он должен выполнить /start."); return
            if recipient["user_id"]==uid: await message.answer("Нельзя отправить подарок самому себе."); return
            item=json.loads(row["waiting_data"] or "{}")
            price=int(item.get("price",0)); gift_name=item.get("name","Подарок")
            if not price: await message.answer("Подарок не выбран. Начни снова с меню."); await clear_waiting(uid); return
            item["recipient_id"]=recipient["user_id"]
            await execute("UPDATE users SET waiting_action='gift_caption',waiting_data=? WHERE user_id=?",
                          (json.dumps(item,ensure_ascii=False),uid))
            await message.answer(f"Добавь подпись к подарку «{gift_name}» (до 300 символов) или нажми «Без подписи».",
                                 reply_markup=kb([[("Без подписи", "giftcaption:skip")]]))
            return
        if action=="gift_caption":
            if len(text)>300: await message.answer("Подпись слишком длинная. Максимум 300 символов."); return
            info=json.loads(row["waiting_data"])
            ok=await deliver_gift(message.bot,uid,message.from_user.first_name,info,text)
            await clear_waiting(uid)
            if not ok: await message.answer("Не хватает фишек для подарка.")
            return
        if action=="duel_user":
            target=await resolve_username(text)
            if not target: await message.answer("Не нашёл пользователя. Он должен запустить бота командой /start."); return
            if target["user_id"]==uid or target["banned"]: await message.answer("Выбери другого доступного игрока."); return
            await execute("UPDATE users SET waiting_action='duel_stake',waiting_data=? WHERE user_id=?",(str(target["user_id"]),uid))
            await message.answer(f"Введи ставку поединка, которую внесёт каждый игрок (минимум 1). Соперник: {target['first_name']}.")
            return
        if action=="duel_stake":
            if not text.isdigit() or int(text)<1: await message.answer("Введи положительную ставку целым числом."); return
            target_id=int(row["waiting_data"]); stake=int(text)
            if not await change_balance(uid,-stake,"duel_stake","Вызов соперника"): await message.answer("Не хватает фишек."); await clear_waiting(uid); return
            cur=await execute("INSERT INTO duels(player1,player2,stake,status,created_at) VALUES(?,?,?,'pending',?)",(uid,target_id,stake,utcnow()))
            await clear_waiting(uid); await message.answer(f"Вызов отправлен! Ставка: {stake} фишек с каждого.")
            try: await message.bot.send_message(target_id,f"⚔️ {message.from_user.first_name} вызывает тебя на поединок! Ставка: {stake} фишек с каждого.",reply_markup=kb([[("Принять",f"duelaccept:{cur.lastrowid}"),("Отклонить",f"duelreject:{cur.lastrowid}")]]))
            except Exception:
                await execute("UPDATE duels SET status='cancelled' WHERE id=? AND status='pending'",(cur.lastrowid,))
                await change_balance(uid,stake,"duel_refund",f"Не доставлен вызов #{cur.lastrowid}")
                await message.answer("Не смог доставить вызов. Ставка возвращена.")
            return

    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        await bot.session.close()
        await DB.close()


if __name__ == "__main__":
    asyncio.run(main())
