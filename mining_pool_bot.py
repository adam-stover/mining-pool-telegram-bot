import asyncio
import aiofiles
import aiohttp
import itertools
import zmq
import zmq.asyncio
import pybtc
import json
import logging
from sys import stdout
from settings import ZMQ_ADDRESS, RPC_ADDRESS, SUBSCRIPTION, POOLS_URL, LOG_FILE, DATA_FILE, BASE_URL, CHAT_ID, HELP_STR

def setup_logging():
    stream_handler = logging.StreamHandler(stdout)
    file_handler = logging.FileHandler(LOG_FILE)
    stream_handler.setLevel(logging.INFO)
    file_handler.setLevel(logging.WARNING)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s -  %(message)s')
    stream_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)
    logging.basicConfig(level=logging.INFO, handlers=[stream_handler, file_handler])

def get_pool_names(pools):
    pool_name_set = {p['name'] for p in list(pools['coinbase_tags'].values()) + list(pools['payout_addresses'].values())}
    pool_names = ' | '.join(sorted(pool_name_set))
    return pool_names

async def get_pools(session):
    async with session.get(POOLS_URL) as resp:
        resp = await resp.json(content_type='text/plain; charset=utf-8')
        return resp

async def send_message(session, body):
    await session.post(f'{BASE_URL}/sendMessage', data=body)

class Store:
    async def read(self):
        async with aiofiles.open(DATA_FILE, 'r') as f:
            data = await f.read()
        data = json.loads(data)
        self.last_block_sent = data['last_block_sent']
        self.offset = data['offset']
        self.pool_subs = data['pool_subs']

    async def _write(self):
        data = json.dumps({'last_block_sent': self.last_block_sent, 'offset': self.offset, 'pool_subs': self.pool_subs})
        async with aiofiles.open(DATA_FILE, 'w') as f:
            await f.write(data)

    def update_offset(self, offset):
        self.offset = offset
        asyncio.create_task(self._write())

    def update_last_block_sent(self, last_block_sent):
        self.last_block_sent = last_block_sent
        asyncio.create_task(self._write())

class BotManager:
    def __init__(self, session, store, pools):
        self.session = session
        self.store = store
        self.pool_names = get_pool_names(pools)

    def parse_commands_from_updates(self, updates):
        commands = []
        offset = -1
        for update in updates:
            if (update['update_id'] >= offset):
                offset = update['update_id'] + 1
            if 'message' in update:
                msg = update['message']
                if msg['chat']['type'] == 'private' and msg['from']['is_bot'] is False:
                    logging.info(f'message: {msg}')
                    if 'entities' in msg:
                        for entity in msg['entities']:
                            if entity['type'] == 'bot_command':
                                begin = entity['offset']
                                end = begin + entity['length']
                                text = msg['text']
                                command = text[begin:end]
                                pool_name = text[end + 1:]
                                commands.append({'chat_id': str(msg['chat']['id']), 'message_id': msg['message_id'], 'cmd': command, 'pool_name': pool_name})
                                break
                else:
                    logging.debug(f'message: {msg}')
        return (commands, offset)

    async def get_updates(self):
        body = {'offset': self.store.offset, 'timeout': 120, 'allowed_updates': ['message']}
        async with self.session.get(f'{BASE_URL}/getUpdates', params=body) as resp:
            resp = await resp.json()
            if resp['ok'] is not True:
                logging.warning(f'Fail: {resp["reason"]}')
                return []
            return resp['result']

    async def send_responses(self, commands):
        if len(commands) == 0:
            return
        tasks = []
        for command in commands:
            chat_id = command['chat_id']
            message_id = command['message_id']
            cmd = command['cmd']
            pool_name = command['pool_name']
            body = {'chat_id': chat_id, 'reply_to_message_id': message_id}
            if cmd == '/start' or cmd == '/help':
                body['text'] = HELP_STR
            elif cmd == '/list':
                body['text'] = self.pool_names
            elif cmd == '/subscribe':
                if pool_name not in self.store.pool_subs:
                    body['text'] = f'Failed to subscribe to {pool_name}: pool not found. Be sure that you have written the pool exactly how it appears in /list (it is case sensitive!) E.g.: /subscribe SlushPool'
                elif chat_id in self.store.pool_subs[pool_name]:
                    body['text'] = f'Failed to subscribe to {pool_name}: you are already subscribed to this pool.'
                else:
                    self.store.pool_subs[pool_name][chat_id] = True
                    body['text'] = f'Successfully subscribed to {pool_name}.'
            elif cmd == '/unsubscribe':
                if pool_name not in self.store.pool_subs:
                    body['text'] = f'Failed to subscribe to {pool_name}: pool not found. Be sure that you have written the pool exactly how it appears in /list (it is case sensitive!) E.g.: /subscribe SlushPool'
                elif chat_id not in self.store.pool_subs[pool_name]:
                    body['text'] = f'Failed to unsubscribe from {pool_name}: you were not subscribed to this pool.'
                else:
                    self.store.pool_subs[pool_name].pop(chat_id)
                    body['text'] = f'Successfully unsubscribed from {pool_name}.'
            elif cmd == '/listsubs':
                user_subs = list()
                for pool in self.store.pool_subs:
                    if chat_id in self.store.pool_subs[pool]:
                        user_subs.append(pool)
                if len(user_subs) == 0:
                    body['text'] = 'You are not subscribed to any pools.'
                else:
                    body['text'] = f'You are subscribed to: {" | ".join(user_subs)}'
            elif cmd == '/clearsubs':
                user_subs = list()
                for pool in self.store.pool_subs:
                    if chat_id in self.store.pool_subs[pool]:
                        user_subs.append(pool)
                        self.store.pool_subs[pool].pop(chat_id)
                if len(user_subs) == 0:
                    body['text'] = 'You were not subscribed to any pools.'
                else:
                    body['text'] = f'Successfully unsubscribed from: {" | ".join(user_subs)}'
            else:
                body['text'] = 'Unknown command.'
            tasks.append(send_message(self.session, body))
        await asyncio.gather(*tasks)

    async def process_updates(self):
        updates = await self.get_updates()
        if len(updates) == 0:
            return -1
        commands, new_offset = self.parse_commands_from_updates(updates)
        await self.send_responses(commands)
        return new_offset

    async def run(self):
        logging.info('Awaiting first new command...')
        while True:
            offset = await self.process_updates()
            if offset > self.store.offset:
                self.store.update_offset(offset)


class StreamManager:
    def __init__(self, session, store, pools):
        self.ctx = zmq.asyncio.Context()
        self.pools = pools
        self.session = session
        self.store = store
        self._next_rpc_id = itertools.count(1).__next__

    def get_coinbase_from_raw_block(self, raw_block):
        block = pybtc.Block(raw_block, format="decoded")
        coinbase = block['tx'][0]
        return coinbase

    def get_reward_from_coinbase(self, coinbase):
        return f"₿{sum(coinbase['vOut'][i]['value'] for i in coinbase['vOut']) / 100000000}"

    def get_miner_from_coinbase(self, coinbase):
        for i in coinbase['vOut']:
            vout = coinbase['vOut'][i]
            if 'address' in vout:
                address = vout['address']
                if len(address) > 0 and address in self.pools['payout_addresses']:
                    return self.pools['payout_addresses'][address]['name']

        coinbaseAscii = bytearray.fromhex(coinbase['vIn'][0]['scriptSig']).decode('utf-8', 'ignore')
        for tag in self.pools['coinbase_tags']:
            if tag in coinbaseAscii:
                return self.pools['coinbase_tags'][tag]['name']

        logging.warning(f'Pool not found: {coinbaseAscii}')
        return 'Unknown'

    async def send_new_block(self, miner, reward, block_count):
        text = f'New block #{block_count} mined by {miner} for {reward}'
        tasks = [send_message(self.session, {"chat_id": CHAT_ID, "text": text})]
        if miner in self.store.pool_subs:
            for chat_id in self.store.pool_subs[miner]:
                body = {"chat_id": chat_id, "text": text}
                tasks.append(send_message(self.session, body))
        await asyncio.gather(*tasks)
        self.store.update_last_block_sent(block_count)
        logging.info(text)

    async def handle_msg(self, msg):
        hex_msg = msg.hex()
        if len(hex_msg) > 16:
            block_count = self.store.last_block_sent + 1
            coinbase = self.get_coinbase_from_raw_block(msg)
            reward = self.get_reward_from_coinbase(coinbase)
            miner = self.get_miner_from_coinbase(coinbase)
            await self.send_new_block(miner, reward, block_count)

    async def query_rpc(self, method, params=[]):
        data = {'jsonrpc': '2.0', 'id': self._next_rpc_id(), 'method': method, 'params': params}
        async with self.session.post(RPC_ADDRESS, json=data) as resp:
            if resp.ok:
                resp = await resp.json()
                return resp['result']
            else:
                logging.warning(f'Unable to query rpc for method {method} with params {params}: {resp.status} -- {resp.reason}')

    async def catch_up_if_necessary(self):
        last_block_sent = self.store.last_block_sent
        actual_last_block = await self.query_rpc('getblockcount')
        if last_block_sent != actual_last_block:
            logging.info(f'{last_block_sent} is different from {actual_last_block}, catching up: ')
            tasks = [self.query_rpc('getblockhash', [h]) for h in range(last_block_sent + 1, actual_last_block + 1)]
            hashes = await asyncio.gather(*tasks)
            tasks = [self.query_rpc('getblock', [h, 0]) for h in hashes]
            blocks = await asyncio.gather(*tasks)
            for i in range(len(blocks)):
                if blocks[i] is not None:
                    coinbase = self.get_coinbase_from_raw_block(blocks[i])
                    reward = self.get_reward_from_coinbase(coinbase)
                    miner = self.get_miner_from_coinbase(coinbase)
                    block_count = last_block_sent + 1 + i
                    await self.send_new_block(miner, reward, block_count)

    async def run(self):
        sock = self.ctx.socket(zmq.SUB)
        sock.connect(ZMQ_ADDRESS)
        sock.subscribe(SUBSCRIPTION)

        await self.catch_up_if_necessary()

        logging.info('Awaiting first new block...')
        while True:
            msg = await sock.recv()
            await self.handle_msg(msg)


async def main():
    store = Store()
    await store.read()
    async with aiohttp.ClientSession() as session:
        pools = await get_pools(session)
        stream_manager = StreamManager(session, store, pools)
        bot_manager = BotManager(session, store, pools)
        await asyncio.gather(stream_manager.run(), bot_manager.run())

if __name__ == '__main__':
    setup_logging()
    logging.info('Starting mining pool bot...')
    asyncio.run(main())
