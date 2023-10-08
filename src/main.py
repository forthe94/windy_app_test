import asyncio

from src.parser import OpenDataParser


async def main():
    parser = OpenDataParser()
    await parser.run()

if __name__ == '__main__':
    asyncio.run(main())
