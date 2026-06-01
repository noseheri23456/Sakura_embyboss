#! /usr/bin/python3
# -*- coding: utf-8 -*-

import asyncio
import uvloop

try:
    asyncio.get_running_loop()
except RuntimeError:
    asyncio.set_event_loop(uvloop.new_event_loop())

from bot import bot

# 面板
from bot.modules.panel import *
# 命令
from bot.modules.commands import *
# 其他
from bot.modules.extra import *
from bot.modules.callback import *
from bot.web import *

bot.run()
