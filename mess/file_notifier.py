import pyinotify,asyncio

async def a():
    while True:
        print("a")
        await asyncio.sleep(10)
def b(event):
    print("b")

def watch():
    wm = pyinotify.WatchManager()
    mask = pyinotify.IN_DELETE | pyinotify.IN_CREATE | pyinotify.IN_MODIFY  # watched events

    wdd = wm.add_watch('/tmp/test', mask)
    notifier = pyinotify.AsyncioNotifier(wm, asyncio.get_running_loop(), callback=b)

async def main_async():
    watch()
    await asyncio.gather(a())