from secrets import TOKEN, RPC_USER, RPC_PASSWORD

ZMQ_ADDRESS = 'tcp://127.0.0.1:28332'
RPC_ADDRESS = 'http://%s:%s@127.0.0.1:8332'%(RPC_USER, RPC_PASSWORD)
SUBSCRIPTION = 'rawblock'
POOLS_URL = 'https://raw.githubusercontent.com/btccom/Blockchain-Known-Pools/master/pools.json'
TELEGRAM_URL = 'https://api.telegram.org/bot'
LOG_FILE = 'pool_bot.log'
DATA_FILE = 'data.json'
BASE_URL = TELEGRAM_URL + TOKEN
CHAT_ID = 0 # Put your chat ID here for channel broadcasts
HELP_STR = """Welcome to the Mining Pool Bot. Usage:

/list
/invite
/subscribe <pool>
/unsubscribe <pool>
/listsubs
/clearsubs

Once subscribed, the bot will DM you every time a pool you are subscribed to finds a block.
To receive notifications for all blocks, use the /invite command and follow the invite link.

Pools are case sensitive.

Examples:
/subscribe SlushPool"""
