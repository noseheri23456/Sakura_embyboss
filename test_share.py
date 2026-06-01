import asyncio
import p115client

async def test():
    c = p115client.P115Client()
    r = await c.share_snap({'share_code': 'swfpkd23hbt', 'receive_code': 'w298'}, async_=True)
    print(r)

if __name__ == '__main__':
    asyncio.run(test())
