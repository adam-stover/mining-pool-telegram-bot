import asyncio
import aiohttp
import zmq
import zmq.asyncio
import pybtc
import json
import logging
from sys import stdout
from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException
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
    async with session.get(BASE_URL + '/sendMessage', params=body) as resp:
        return resp.status


class Store:
    def __init__(self):
        self._read()

    def _read(self):
        with open(DATA_FILE, 'r') as f:
            data = f.read()
            data = json.loads(data)
            self.last_block_sent = data['last_block_sent']
            self.offset = data['offset']
            self.pool_subs = data['pool_subs']

    def _write(self):
        with open(DATA_FILE, 'w') as f:
            data = json.dumps({'last_block_sent': self.last_block_sent, 'offset': self.offset, 'pool_subs': self.pool_subs})
            f.write(data)

    def update_offset(self, offset):
        self.offset = offset
        self._write()

    def update_last_block_sent(self, last_block_sent):
        self.last_block_sent = last_block_sent
        self._write()


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
                logging.info('message: %s', msg)
                if msg['chat']['type'] == 'private' and msg['from']['is_bot'] is False and 'entities' in msg:
                    for entity in msg['entities']:
                        if entity['type'] == 'bot_command':
                            begin = entity['offset']
                            end = begin + entity['length']
                            text = msg['text']
                            command = text[begin:end]
                            pool_name = text[end + 1:]
                            commands.append({'chat_id': str(msg['chat']['id']), 'message_id': msg['message_id'], 'cmd': command, 'pool_name': pool_name})
                            break
        return (commands, offset)

    async def get_updates(self):
        body = {'offset': self.store.offset, 'timeout': 120, 'allowed_updates': ['message']}
        async with self.session.get(BASE_URL + '/getUpdates', params=body) as resp:
            resp = await resp.json()
            if resp['ok'] is not True:
                logging.warning('Fail: %s', resp.text)
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
                    body['text'] = 'Failed to subscribe to %s: pool not found. Be sure that you have written the pool exactly how it appears in /list (it is case sensitive!) E.g.: /subscribe SlushPool'%(pool_name)
                elif chat_id in self.store.pool_subs[pool_name]:
                    body['text'] = 'Failed to subscribe to %s: you are already subscribed to this pool.'%(pool_name)
                else:
                    self.store.pool_subs[pool_name][chat_id] = True
                    body['text'] = 'Successfully subscribed to %s.'%(pool_name)
            elif cmd == '/unsubscribe':
                if pool_name not in self.store.pool_subs:
                    body['text'] = 'Failed to subscribe to %s: pool not found. Be sure that you have written the pool exactly how it appears in /list (it is case sensitive!) E.g.: /subscribe SlushPool'%(pool_name)
                elif chat_id not in self.store.pool_subs[pool_name]:
                    body['text'] = 'Failed to unsubscribe from %s: you were not subscribed to this pool.'%(pool_name)
                else:
                    self.store.pool_subs[pool_name].pop(chat_id)
                    body['text'] = 'Successfully unsubscribed from %s.'%(pool_name)
            elif cmd == '/listsubs':
                user_subs = list()
                for pool in self.store.pool_subs:
                    if chat_id in self.store.pool_subs[pool]:
                        user_subs.append(pool)
                if len(user_subs) == 0:
                    body['text'] = 'You are not subscribed to any pools.'
                else:
                    body['text'] = 'You are subscribed to: ' + ' | '.join(user_subs)
            elif cmd == '/clearsubs':
                user_subs = list()
                for pool in self.store.pool_subs:
                    if chat_id in self.store.pool_subs[pool]:
                        user_subs.append(pool)
                        self.store.pool_subs[pool].pop(chat_id)
                if len(user_subs) == 0:
                    body['text'] = 'You were not subscribed to any pools.'
                else:
                    body['text'] = 'Successfully unsubscribed from: ' + ' | '.join(user_subs)
            else:
                body['text'] = 'Unknown command.'
            tasks.append(asyncio.create_task(send_message(self.session, body)))
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

    def get_miner_from_raw_block(self, raw_block):
        block = pybtc.Block(raw_block, format="decoded")
        coinbase = block['tx'][0]

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

        logging.info('Pool not found: %s', coinbaseAscii)
        return 'Unknown'

    async def send_new_block(self, miner, block_count):
        text = 'New block #%s mined by: %s'%(block_count, miner)
        tasks = [asyncio.create_task(send_message(self.session, {"chat_id": CHAT_ID, "text": text}))]
        if miner in self.store.pool_subs:
            for chat_id in self.store.pool_subs[miner].keys():
                body = {"chat_id": chat_id, "text": text}
                tasks.append(asyncio.create_task(send_message(self.session, body)))
        await asyncio.gather(*tasks)
        self.store.update_last_block_sent(block_count)
        logging.info(text)

    async def handle_msg(self, msg):
        hex_msg = msg.hex()
        if len(hex_msg) > 16:
            block_count = self.store.last_block_sent + 1
            miner = self.get_miner_from_raw_block(msg)
            await self.send_new_block(miner, block_count)
    
    async def catch_up_if_necessary(self):
        last_block_sent = self.store.last_block_sent
        rpc = AuthServiceProxy(RPC_ADDRESS)
        actual_last_block = rpc.getblockcount()
        if last_block_sent != actual_last_block:
            logging.info('%s is different from %s, catching up...', last_block_sent, actual_last_block)
            cmds = [['getblockhash', height] for height in range(last_block_sent + 1, actual_last_block + 1)]
            block_hashes = rpc.batch_(cmds)
            blocks = rpc.batch_([['getblock', h, 0] for h in block_hashes])
            for i in range(len(blocks)):
                miner = self.get_miner_from_raw_block(blocks[i])
                block_count = last_block_sent + 1 + i
                await self.send_new_block(miner, block_count)

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
    async with aiohttp.ClientSession() as session:
        pools = await get_pools(session)
        stream_manager = StreamManager(session, store, pools)
        bot_manager = BotManager(session, store, pools)
        await asyncio.gather(stream_manager.run(), bot_manager.run())

if __name__ == '__main__':
    setup_logging()
    logging.info('Starting mining pool bot...')
    asyncio.run(main())
