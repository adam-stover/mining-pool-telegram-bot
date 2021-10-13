import asyncio
import aiofiles
import aiohttp
import itertools
import zmq
import zmq.asyncio
import pybtc
import json
import logging
import sys
from settings import ZMQ_ADDRESS, RPC_ADDRESS, SUBSCRIPTION, POOLS_URL, LOG_FILE, DATA_FILE, BASE_URL, CHAT_ID, HELP_STR


def setup_logging():
    """Handles logging."""
    stream_handler = logging.StreamHandler(sys.stdout)
    file_handler = logging.FileHandler(LOG_FILE)
    stream_handler.setLevel(logging.INFO)
    file_handler.setLevel(logging.WARNING)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s -  %(message)s')
    stream_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)
    logging.basicConfig(level=logging.INFO, handlers=[stream_handler, file_handler])


class Store:
    """Responsible for managing state during application run-time, and persisting this data beyond run-time."""
    def __init__(self):
        """Initializes the Store, gets inital data values"""
        self.pool_subs = None
        self.offset = None
        self.last_block_sent = None
        self._read()

    async def _get_pools(self, session):
        """Retrieves pool information (known payout addresses and tags) from a known static source."""
        async with session.get(POOLS_URL) as resp:
            self.pools = await resp.json(content_type='text/plain; charset=utf-8')

    async def _read(self):
        """Loads the store with data from persistent file storage."""
        async with aiofiles.open(DATA_FILE, 'r') as f:
            data = await f.read()
        data = json.loads(data)
        self.last_block_sent = data['last_block_sent']
        self.offset = data['offset']
        self.pool_subs = data['pool_subs']

    async def _write(self):
        """Writes the store's current state to persistent file storage."""
        data = json.dumps({'last_block_sent': self.last_block_sent, 'offset': self.offset, 'pool_subs': self.pool_subs})
        async with aiofiles.open(DATA_FILE, 'w') as f:
            await f.write(data)

    def update_offset(self, offset):
        """Updates offset. Non-blocking."""
        self.offset = offset
        asyncio.create_task(self._write())

    def update_last_block_sent(self, last_block_sent):
        """Updates the last block sent. Non-blocking."""
        self.last_block_sent = last_block_sent
        asyncio.create_task(self._write())

    async def load(self, session):
        """Retrieves pool information from static source, and loads data from file storage."""
        await asyncio.gather(self._get_pools(session), self._read())


class BotManager:
    """
    Responsible for Telegram integration -- receiving and sending messages.
    To follow the flow, start with __init__, then the `run` method below.
    See ttps://core.telegram.org/bots/api for more info.
    """

    def __init__(self, session, store):
        """Initializes the BotManager."""
        self._session = session
        self._store = store
        pool_name_set = {p['name'] for p in list(self._store.pools['coinbase_tags'].values()) + list(
            self._store.pools['payout_addresses'].values())}
        self._poolnames = ' | '.join(sorted(pool_name_set))
        self._channel_invite_link = ''

    @staticmethod
    def _parse_commands_from_updates(_self, updates):
        """Parses updates received into actionable user commands."""
        commands = []
        offset = -1
        for update in updates:
            logging.debug(update)
            if update['update_id'] >= offset:
                offset = update['update_id'] + 1
            # Ignore non-messages
            if 'message' in update:
                msg = update['message']
                # Ignore group chats, other bots, and non-text messages
                if msg['chat']['type'] == 'private' and msg['from']['is_bot'] is False and 'text' in msg:
                    logging.info(f'{msg["date"]} -- {msg["from"]} -- {msg["text"]}')
                    if 'entities' in msg:
                        for entity in msg['entities']:
                            if entity['type'] == 'bot_command':
                                begin = entity['offset']
                                end = begin + entity['length']
                                text = msg['text']
                                command = text[begin:end]
                                pool_name = text[end + 1:]
                                commands.append(
                                    {'chat_id': str(msg['chat']['id']), 'message_id': msg['message_id'], 'cmd': command,
                                     'pool_name': pool_name})
                                break
        return commands, offset

    def _clear_subs(self, chat_id):
        """Unsubscribes a user from all subscriptions -- returns their former subscriptions."""
        user_subs = list()
        for pool in self._store.pool_subs:
            if chat_id in self._store.pool_subs[pool]:
                user_subs.append(pool)
                # This is not ideal. However, this function is always followed with a call to update the store.
                # I do it this way to avoid excessive writes to the filesystem.
                self._store.pool_subs[pool].remove(chat_id)
        return user_subs

    async def _post(self, route, body, default_value=None):
        """Generic method for contacting Telegram Bot API."""
        async with self._session.post(f'{BASE_URL}/{route}', data=body) as resp:
            if not resp.ok:
                logging.warning(f'Fail to hit {route} with {body} -- {resp.status} -- {resp.reason}')
                if default_value is not None:
                    return resp.status
                return default_value
            elif default_value is not None:
                parsed = await resp.json()
                return parsed['result']

    async def _get_updates(self):
        """Gets relevant updates from Telegram. See https://core.telegram.org/bots/api#getupdates"""
        body = {'offset': self._store.offset, 'timeout': 120, 'allowed_updates': ['message']}
        return await self._post('getUpdates', body, [])

    async def _get_invite_link(self):
        """Retrieves an invite link for the broadcast channel. Creates if it doesn't exist."""
        if self._channel_invite_link == '':
            self._channel_invite_link = await self._post('exportChatInviteLink', {'chat_id': CHAT_ID}, '')

        return self._channel_invite_link

    @staticmethod
    async def _cmd_help(_self, _command):
        return HELP_STR

    async def _cmd_list(self, _command):
        return self._poolnames

    async def _cmd_invite(self, _command):
        return await self._get_invite_link()

    async def _cmd_subscribe(self, command):
        pool_name = command['pool_name']
        chat_id = command['chat_id']

        if pool_name == '':
            return f'Failed to subscribe. You must enter a pool to subscribe. E.g.: /subscribe SlushPool'
        elif pool_name not in self._store.pool_subs:
            return f'Failed to subscribe to {pool_name}: pool not found. Be sure that you have written the pool exactly how it appears in /list (it is case sensitive!) E.g.: /subscribe SlushPool'
        elif chat_id in self._store.pool_subs[pool_name]:
            return f'Failed to subscribe to {pool_name}: you are already subscribed to this pool.'
        else:
            self._store.pool_subs[pool_name].append(chat_id)
            return f'Successfully subscribed to {pool_name}.'

    async def _cmd_unsubscribe(self, command):
        pool_name = command['pool_name']
        chat_id = command['chat_id']

        if pool_name not in self._store.pool_subs:
            return f'Failed to subscribe to {pool_name}: pool not found. Be sure that you have written the pool exactly how it appears in /list (it is case sensitive!) E.g.: /subscribe SlushPool'
        elif chat_id not in self._store.pool_subs[pool_name]:
            return f'Failed to unsubscribe from {pool_name}: you were not subscribed to this pool.'
        else:
            self._store.pool_subs[pool_name].remove(chat_id)
            return f'Successfully unsubscribed from {pool_name}.'

    async def _cmd_listsubs(self, command):
        chat_id = command['chat_id']

        user_subs = []
        for pool in self._store.pool_subs:
            if chat_id in self._store.pool_subs[pool]:
                user_subs.append(pool)
        if len(user_subs) == 0:
            return 'You are not subscribed to any pools.'
        else:
            return f'You are subscribed to: {" | ".join(user_subs)}'

    def _cmd_clearsubs(self, command):
        chat_id = command['chat_id']

        user_subs = self._clear_subs(chat_id)
        if len(user_subs) == 0:
            return 'You were not subscribed to any pools.'
        else:
            return f'Successfully unsubscribed from: {" | ".join(user_subs)}'

    async def _send_response(self, command):
        """Logic to react and respond to user messages. Ugly, will refactor to use dict."""
        cmd = command['cmd']
        allowed_commands = {
            '/start': self._cmd_help,
            '/help': self._cmd_help,
            '/list': self._cmd_list,
            '/invite': self._cmd_invite,
            '/subscribe': self._cmd_subscribe,
            '/unsubscribe': self._cmd_unsubscribe,
            '/listsubs': self._cmd_listsubs,
            '/clearsubs': self._cmd_clearsubs,
        }
        if cmd in allowed_commands:
            body = {'chat_id': command['chat_id'], 'reply_to_message_id': command['message_id'], 'text': await allowed_commands[cmd](command)}
        else:
            body = {'chat_id': command['chat_id'], 'reply_to_message_id': command['message_id'], 'text': 'Unknown command.'}

        await self._post('sendMessage', body)

    async def _process_updates(self):
        """Processes updates, our responses to updates, and returns the new offset."""
        updates = await self._get_updates()
        if len(updates) == 0:
            return -1
        commands, new_offset = self._parse_commands_from_updates(updates)
        tasks = [asyncio.create_task(self._send_response(command)) for command in commands]
        await asyncio.gather(*tasks)
        return new_offset

    async def send_message(self, body):
        """Public method for sending specific messages to channels/users. If forbidden, remove that user's subscriptions."""
        status = await self._post('sendMessage', body)
        if status == 403 and 'chat_id' in body:
            chat_id = body['chat_id']
            subs = self._clear_subs(chat_id)
            logging.info(f'Cleared {subs} from {chat_id}')

    async def run(self):
        """Loop to make it all happen."""
        logging.info('Awaiting first new command...')
        while True:
            offset = await self._process_updates()
            if offset > self._store.offset:
                # This ensures that any changes made to the Store while processing updates gets persisted.
                self._store.update_offset(offset)


class StreamManager:
    """
    Class for receiving and handling block updates from a Bitcoin node.
    To follow the flow, start with __init__ then move to `run` below.
    See https://github.com/bitcoin/bitcoin/blob/master/doc/zmq.md for more info.
    """

    def __init__(self, store, bot):
        """Initializes ZMQ context, store, bot, RPC iterator."""
        self._ctx = zmq.asyncio.Context()
        self._store = store
        self._bot = bot
        self._next_rpc_id = itertools.count(1).__next__

    def _get_miner_from_coinbase(self, coinbase):
        """
        Retrieves miner from coinbase based on known payout addresses and tags.
        Coinbase tx comes in decoded format when received over RPC (only when catching up from downtime).
        From ZMQ it will come in raw format which is more performant.
        """
        if coinbase['format'] == 'decoded':
            vouts = coinbase['vOut']
        else:
            vouts = coinbase.decode()['vOut']

        for i in vouts:
            vout = vouts[i]
            if 'address' in vout:
                address = vout['address']
                if len(address) > 0 and address in self._store.pools['payout_addresses']:
                    logging.debug(f'Found miner from payout address {i}')
                    return self._store.pools['payout_addresses'][address]['name']

        if coinbase['format'] == 'decoded':
            coinbaseAscii = bytearray.fromhex(coinbase['vIn'][0]['scriptSig']).decode('utf-8', 'ignore')
        else:
            coinbaseAscii = coinbase['vIn'][0]['scriptSig'].decode('utf-8', 'ignore')

        for tag in self._store.pools['coinbase_tags']:
            if tag in coinbaseAscii:
                logging.debug(f'Found miner from tag {tag}')
                return self._store.pools['coinbase_tags'][tag]['name']

        logging.warning(f'Pool not found: {coinbaseAscii}')
        return 'Unknown'

    def _get_miner_and_reward_from_msg(self, msg):
        """Parses miner and miner reward from msg (either bytes from ZMQ or post-processed from RPC)."""
        coinbase = pybtc.Block(msg, format='raw')['tx'][0]
        miner = self._get_miner_from_coinbase(coinbase)
        reward = f"₿{format(sum(coinbase['vOut'][i]['value'] for i in coinbase['vOut']) / 100000000, '.8f')}"
        return miner, reward

    async def _send_new_block(self, miner, reward, block_count):
        """Sends block notification to broadcast channel and all subscribed users."""
        text = f'New block #{block_count} mined by {miner} for {reward}'
        colos = [self._bot.send_message({"chat_id": CHAT_ID, "text": text})]
        if miner in self._store.pool_subs:
            for chat_id in self._store.pool_subs[miner]:
                body = {"chat_id": chat_id, "text": text}
                colos.append(self._bot.send_message(body))
        # Batch to avoid Telegram rate limiting. Docs say 30/1s but setting to 20 to be safe.
        await batch_colos(20, colos)
        # Ensures last block sent gets persisted. Unused while running, but crucial for recovering from downtime automatically.
        self._store.update_last_block_sent(block_count)
        logging.info(text)

    async def _handle_msg(self, msg):
        """Handles new blocks -- ignores the metadata messages that ZMQ also sends. Empty block is around 300 bytes so 64 is a sufficient threshold."""
        if len(msg) > 64:
            block_count = self._store.last_block_sent + 1
            miner, reward = self._get_miner_and_reward_from_msg(msg)
            await self._send_new_block(miner, reward, block_count)

    async def _query_rpc(self, session, method, params=[]):
        """General method for querying RPC server. This is only for catching up from downtime."""
        data = {'jsonrpc': '2.0', 'id': self._next_rpc_id(), 'method': method, 'params': params}
        async with session.post(RPC_ADDRESS, json=data) as resp:
            if resp.ok:
                resp = await resp.json()
                return resp['result']
            else:
                logging.warning(
                    f'Unable to query rpc for method {method} with params {params}: {resp.status} -- {resp.reason}')

    async def catch_up_if_necessary(self, session):
        """If there was downtime, we want to send any notifications we missed. This checks if we missed any blocks, and processes them if necessary."""
        last_block_sent = self._store.last_block_sent
        actual_last_block = await self._query_rpc(session, 'getblockcount')
        if last_block_sent != actual_last_block:
            logging.info(f'{last_block_sent} is different from {actual_last_block}, catching up: ')
            tasks = [self._query_rpc(session, 'getblockhash', [h]) for h in
                     range(last_block_sent + 1, actual_last_block + 1)]
            # Batch in groups of 10 to avoid rate limiting errors.
            hashes = await batch_colos(10, tasks)
            tasks = [self._query_rpc(session, 'getblock', [h, 0]) for h in hashes]
            blocks = await batch_colos(10, tasks)
            for i in range(len(blocks)):
                if blocks[i] is not None:
                    await self._handle_msg(blocks[i])

    async def _handle_multipart(self, parts):
        """Entry for new messages over ZMQ."""
        tasks = [asyncio.create_task(self._handle_msg(part)) for part in parts]
        await asyncio.gather(*tasks)

    async def run(self):
        """Loop to make it all happen."""
        sock = self._ctx.socket(zmq.SUB)
        sock.connect(ZMQ_ADDRESS)
        sock.subscribe(SUBSCRIPTION)

        logging.info('Awaiting first new block...')
        while True:
            msg = await sock.recv_multipart()
            await self._handle_multipart(msg)


async def batch_colos(batch_size, colos):
    """Helper to batch out-going requests to avoid rate-limiting errors from RPC node / Telegram API."""
    i = 0
    j = batch_size

    result = []

    while i < len(colos):
        tasks = [asyncio.create_task(colo) for colo in colos[i:j]] + [asyncio.sleep(1)]
        result += await asyncio.gather(*tasks)
        i += batch_size
        j += batch_size

    return result


async def main():
    """Entry point for app. Shares single store and session across application."""
    store = Store()
    async with aiohttp.ClientSession() as session:
        await store.load(session)
        bot_manager = BotManager(session, store)
        stream_manager = StreamManager(store, bot_manager)
        await asyncio.gather(stream_manager.catch_up_if_necessary(session), stream_manager.run(), bot_manager.run())


if __name__ == '__main__':
    setup_logging()
    logging.info('Starting mining pool bot...')
    asyncio.run(main())
