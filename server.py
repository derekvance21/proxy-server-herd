import asyncio
import json
import aiohttp
import sys
import re
import re
import time
from decimal import Decimal
from env import GOOGLE_PLACES_API_KEY

GOOGLE_PLACES_API_URL="https://maps.googleapis.com/maps/api/place/nearbysearch/json"
SERVERS = ["Riley", "Jaquez", "Bernard", "Juzang", "Campbell"]
PORTS = [x for x in range(15700, 15705)] # [15700, 15704]
SERVER_TO_PORT = dict(zip(SERVERS, PORTS))
PORT_TO_SERVER = dict(zip(PORTS, SERVERS))

SERVER_TO_NEIGHBORS = {
    "Riley": ["Jaquez", "Juzang"],
    "Jaquez": ["Riley", "Bernard"],
    "Bernard": ["Jaquez", "Juzang", "Campbell"],
    "Juzang": ["Riley", "Bernard", "Campbell"],
    "Campbell": ["Bernard", "Juzang"]
}

FORMATS = {
    "IAMAT": ["name", "client", "loc", "time"],
    "AT": ["name", "server", "diff", "client", "loc", "time", "src"],
    "WHATSAT": ["name", "client", "radius", "limit"]
}

clients = {}

def parse_msg(msg):
    try:
        fields = re.split("\s+", msg.strip()) 
        name = fields[0]
        # if number of fields for name doesn't match number of fields in command, raise exception
        if len(fields) != len(FORMATS.get(name)):
            raise Exception(msg)

        cmd = dict(zip(FORMATS.get(name), fields))
        
        ## type checks/conversions
        if name in ["IAMAT", "AT"]:
            cmd["time"] = Decimal(cmd.get("time"))
            cmd["loc"] = tuple(map(lambda x: float(x), filter(lambda x: x, re.findall(r"([-\+]\d+\.?\d*)", cmd.get("loc")))))
            if len(cmd["loc"]) != 2 or not (-90 <= cmd["loc"][0] <= 90 and -180 <= cmd["loc"][1] <= 180):
                raise Exception(msg)
        if name in ["AT"]:
            cmd["diff"] = Decimal(cmd.get("diff"))
        if name in ["WHATSAT"]:
            cmd["radius"] = int(cmd.get("radius"))
            cmd["limit"] = int(cmd.get("limit"))
            if cmd["radius"] > 50 or cmd["limit"] > 20:
                raise Exception(msg)

        return cmd
    
    except:
        raise Exception(msg)


async def output_msg(writer, msg):
    writer.write(msg.encode())
    await writer.drain()
    writer.close()
    log_io(msg, writer.get_extra_info('peername'), "To")


def log_io(msg, addr, head):
    log_info(f"{head} {PORT_TO_SERVER.get(addr[1]) or addr}: {msg!r}")
    

def log_info(msg):
    with open(f"{server_name}.log", "a") as log:
        log.write(f"{msg}\n")


def check_to_propagate(cmd):
    client = clients.get(cmd["client"])
    # old message: stored client data is at least as new as this command
    if not client or client["time"] < cmd["time"]:
        clients.update({cmd["client"]: {"server": cmd["server"], "loc": cmd["loc"], "time": cmd["time"], "diff": cmd["diff"]}})
        return True
    else:
        return False


async def forward_AT(msg, neighbor):
    try:
        reader, writer = await asyncio.open_connection('localhost', SERVER_TO_PORT.get(neighbor))
        await output_msg(writer, msg)

    except ConnectionRefusedError:
        log_info(f"Failed to {neighbor}: {msg!r}")


async def propagate(msg, src=None):
    forwards = (forward_AT(msg, neighbor) for neighbor in SERVER_TO_NEIGHBORS.get(server_name) if neighbor != src)
    await asyncio.gather(*forwards)

    
def create_AT(server, diff, client, loc, time, src=None):
    loc = ''.join(map(lambda x: f"{'+' if x >= 0 else ''}{x}", loc))
    diff = f"{'+' if diff >= 0 else ''}{diff}"
    return f"AT {server} {diff} {client} {loc} {time} {src or ''}".rstrip() + "\n"


async def handle_IAMAT(writer, cmd):
    diff = Decimal(time.time_ns()) / 1000000000 - cmd['time']
    cmd.update({"diff": diff, "server": server_name})
    msg = create_AT(server_name, diff, cmd['client'], cmd['loc'], cmd['time'])
    await output_msg(writer, msg)

    if check_to_propagate(cmd):
        await propagate(f"{msg.rstrip()} {server_name}\n")
        

async def handle_AT(writer, cmd):
    if check_to_propagate(cmd):
        msg = create_AT(cmd['server'], cmd['diff'], cmd['client'], cmd['loc'], cmd['time'], src=server_name)
        await propagate(msg, src=cmd["src"])


async def handle_WHATSAT(writer, cmd):
    client = clients.get(cmd['client'])
    if not client:
        await output_msg(writer, f"? {' '.join(map(lambda x: str(x), cmd.values()))}\n")
    else:
        msg = create_AT(client['server'], client['diff'], cmd['client'], client['loc'], client['time'])
        async with aiohttp.ClientSession() as session:
            params = [('radius', cmd['radius'] * 1000), ('key', GOOGLE_PLACES_API_KEY), ('location', ','.join(map(lambda x: str(x), client['loc'])))]
            async with session.get(GOOGLE_PLACES_API_URL, params=params) as response:
                text = await response.text()
                data = json.loads(text)
                results = data.get("results")
                if results:
                    data.update({"results": results[:cmd['limit']]})

                msg += re.sub(r"\n+", "\n", json.dumps(data, sort_keys=True, indent=3).rstrip()) + "\n\n"
                await output_msg(writer, msg)


HANDLES = {
    "IAMAT": handle_IAMAT,
    "AT": handle_AT,
    "WHATSAT": handle_WHATSAT
}

async def handle_msg(reader, writer):
    data = await reader.read()
    msg = data.decode()
    log_io(msg, writer.get_extra_info('peername'), "From")

    try:
        cmd = parse_msg(msg)
    except:
        await output_msg(writer, f"? {msg}\n")
        return

    handle_cmd = HANDLES.get(cmd["name"])
    await handle_cmd(writer, cmd)


async def main(server_name):
    server = await asyncio.start_server(handle_msg, 'localhost', SERVER_TO_PORT.get(server_name))
    addr = server.sockets[0].getsockname()

    log_info(f"Server going up on {addr}")

    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    try:
        server_name = sys.argv[1]
    except IndexError:
        raise SystemExit("Usage: server.py <server_name>")

    if server_name not in SERVER_TO_PORT or len(sys.argv) != 2:
        raise SystemExit("Usage: server.py <server_name>")

    server_name = sys.argv[1]

    try:
        asyncio.run(main(server_name))

    except KeyboardInterrupt:
        log_info(f"Server going down\n")
        sys.exit(0)
