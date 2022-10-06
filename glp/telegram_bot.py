from aiogram import Bot, Dispatcher, executor, types
from glp_strat_computation_telegram_bot import *
from aiogram.types import InputFile
from data_chart import *
async def compute_handler(message: types.Message):
   try:
      new_message = message.text.split(' ')
      new_usdc = 0
      if len(new_message) > 1:
         new_usdc = float(new_message[1])
       
      output = compute_strat(new_usdc)
   except Exception as E:
      print(E)
      output = "compute/start command failed"
     
   await message.answer(output)

async def test(message: types.Message):
   await message.answer("this is a test")

async def pnlChart(message: types.Message):
   savePnlChart()
   chart = InputFile("pnl.png")
   await message.answer_photo(chart)
async def pnlChartNoArtefacts(message: types.Message):
   savePnlChart(removeArtefacts=True)
   chart = InputFile("pnl.png")
   await message.answer_photo(chart)
async def pnlOnlyChart(message: types.Message):
   savePnlChart(pnlOnly=True)
   chart = InputFile("pnl.png")
   await message.answer_photo(chart)
async def pnlOnlyChartNA(message: types.Message):
   savePnlChart(pnlOnly=True,removeArtefacts=True)
   chart = InputFile("pnl.png")
   await message.answer_photo(chart)
async def send_data(message: types.Message):
   try:
      data = InputFile("data/data.json")
   except:
      data = "compute/start command failed" 
   await message.answer_document(data)
async def main():
   BOT_TOKEN = "5437711089:AAEHetzPcQcNAlCAnfKLTjlW17WHUkVWcY8"
   bot = Bot(token=BOT_TOKEN)
   try:
      disp = Dispatcher(bot=bot)
      disp.register_message_handler(compute_handler, commands={"start", "compute"})
      disp.register_message_handler(send_data, commands={"data"})
      disp.register_message_handler(test, commands={"test"})
      disp.register_message_handler(pnlChart, commands={"pnl"})
      disp.register_message_handler(pnlChartNoArtefacts, commands={"pnlNA"})

      disp.register_message_handler(pnlOnlyChart, commands={"pnlOnly"})
      disp.register_message_handler(pnlOnlyChartNA, commands={"pnlOnlyNA"})

      await disp.start_polling()
   finally:
      await bot.close()


asyncio.run(main())