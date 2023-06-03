import logging
from telegram import Update
from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler
from timeloop import Timeloop
from datetime import timedelta
import multiprocessing as mp
import time
import asyncio
import Misso.services.helper as ph
import threading

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

token = ""
chat_id = ""

MSG_QUEUE = []
INT_LIST = []


def start_loop():
    tl = Timeloop()

    @tl.job(interval=timedelta(seconds=0.5))
    def handle_time():
        if len(MSG_QUEUE) > 0:
            msg = MSG_QUEUE.pop(0)
            global INT_LIST
            INT_LIST.append(msg)
            print(msg)

    tl.start()

async def change(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global MSG_QUEUE
    msg = ["set"]
    for arg in context.args:
        msg.append(arg)
    MSG_QUEUE.append(msg)
    await context.bot.send_message(chat_id=update.effective_chat.id, text="resetting value")

async def get_state(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global MSG_QUEUE
    msg = ["get"]
    for arg in context.args:
        msg.append(arg)
    MSG_QUEUE.append(msg)
    await context.bot.send_message(chat_id=update.effective_chat.id, text=f"Getting Value for {msg} ")

def run_telegram_handler():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    application = ApplicationBuilder().token(token).build()
    change_handler = CommandHandler('set', change)
    get_handler = CommandHandler('get', get_state)
    application.add_handler(change_handler)
    application.add_handler(get_handler)
    start_loop()
    application.run_polling()

def run_telegram_handler_thread():
    tm_handler_thread = threading.Thread(target=run_telegram_handler, args=( ))
    tm_handler_thread.start()

def send_response(msg):
    ph.telegram_notify(msg, token=token, chat_id=chat_id, reformat=True)

# class Spibo:
#     def __init__(self):
#         self.positions = {"CEL":{"status":True, "size":399.08, "ref":[35, 38]}, "USD":{"status":True, "size":399.08, "ref":[35, 38]}, "BTC":{"status":False, "size":39099.08, "ref":[35900, 38800]}}
#
#     def tm_get(self, msg):
#         attr = getattr(self, msg[1])
#         print(attr[msg[2]])
#         return attr[msg[2]]
#
#     def tm_set(self, msg, _type = None):
#         for i in msg:
#             if i.startswith("type:"):
#                 _type = i.split(":")[1]
#         if isinstance(getattr(self, msg[1]), dict):
#             if msg[1] == "positions":
#                 market = msg[2]
#                 key = msg[3]
#                 value = ph.set_value_type(msg[4], _type) if _type is not None else msg[4]
#                 self.positions[market][key] = value
#         else:
#             value = ph.set_value_type(msg[2], _type) if _type is not None else msg[2]
#             setattr(self, msg[1], value)

if __name__ == '__main__':
    run_telegram_handler_thread()
    while True:
        if len(INT_LIST) > 0:
            msg = INT_LIST.pop(0)
            print(f"found msg on INT list", msg)
            if msg[0] == "get":
                resp = spibo.get_tm(msg)
                send_response(str(resp))
            elif msg[0] == "set":
                spibo.set_tm(msg)
                resp = f"setting attribute {msg[1:]}"
                send_response(str(resp))



    # with mp.Manager() as manager:
    #     interactor_list = manager.list()
    #     p1 = mp.Process(target=task, args=(interactor_list, ), name="[HFT]")
    #     p2 = mp.Process(target=run_telegram_handler, args=(interactor_list, ), name="[TM]")



