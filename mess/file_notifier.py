import pyinotify,asyncio

'''
file bus
'''

class MyFileSocket():
    def __init__(self,filename,func):
        self.filename = filename
        self.subscribers = [func]
    def process_message(self,event: pyinotify.Event):
        message = asyncio.create_task(async_read_csv(self.filename,usecols=['name','optimalCoin']))
        for func in self.subscribers:
            status = func(message)

            #self.inventory_target.read_update(message)

def monitor_inventory_target(self, weights):
    # set up pyinotify stuff
    exchange = weights['exchange'].unique()[0]
    subaccount = weights['subaccount'].unique()[0]

    filename = os.path.join(os.sep, configLoader._config_folder_path, 'prod', 'pfoptimizer',
                            f'weights_{exchange}_{subaccount}.csv')
    my_file_watch = MyFileSocket(filename, self.read_source)

    wm = pyinotify.WatchManager()
    mask = pyinotify.IN_DELETE | pyinotify.IN_CREATE  | pyinotify.IN_MODIFY # watched events

    wdd = wm.add_watch(filename, mask)
    notifier = pyinotify.AsyncioNotifier(wm, asyncio.get_running_loop(), callback=my_file_watch.process_message)

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