import getopt
import sys
import csv
from datetime import datetime
from multiprocessing.queues import Queue
from pynput import keyboard
from multiprocessing import Manager, Process, Lock
import signal
import websocket
import json
from collections import deque

# Constants
WINDOW_SIZE = 200

try:
    import thread
except ImportError:
    import _thread as thread


class GracefulExit(Exception):
    pass


def exit_handler(signum, frame):
    raise GracefulExit()


def on_message(ws, message, queues_dict):
    parsed_message = json.loads(message)
    global_queue = queues_dict.get(parsed_message.get('product_id', None), None)
    if parsed_message.get("type", None) == 'match' and global_queue:
        global_queue.put(parsed_message)


def on_error(ws, error):
    print(error)


def on_close(ws):
    print("Closing Websocket connection")


def on_open(ws: websocket.WebSocket, msg: dict):
    def run(*args):
        ws.send(json.dumps(msg))
    Process(target=run,).start()


def get_float_value(obj, key):
    return float(obj.get(key, 0))


def calculate_vwap(cumulative_numerator, cumulative_denominator, new_val, old_val=None):
    """
    :param cumulative_numerator: the numerator is the product of the price and volume of a match
    :param cumulative_denominator: the denominator is the summation of the current window's matches volumes
    :param new_val: the new value to add
    :param old_val: OPTIONAL. Once the window is full (200 values), old val will be the value that's leaving the window
    :return: a tuple containing (new_cumulative_numerator, new_cumulative_denominator, vwap_value)
    """
    new_price = get_float_value(new_val, 'price')
    new_volume = get_float_value(new_val, 'size')
    old_price, old_volume = 0,0
    if old_val:
        old_price = get_float_value(old_val, 'price')
        old_volume = get_float_value(old_val, 'size')
    new_cumulative_denominator = cumulative_denominator + new_volume - old_volume
    new_cumulative_numerator = cumulative_numerator + (new_price * new_volume) - (old_price * old_volume)
    return new_cumulative_numerator, new_cumulative_denominator, new_cumulative_numerator / new_cumulative_denominator


def vwap_task(process_queue: Queue, prod_name: str, l: Lock):
    """
    The VWAP task is what each process will use for calculating the corresponding value as messages are put
    in the queue between said subprocess and the websocket main process.
    Task will output values through stdout as well as in a csv file.
    The sliding window is implemented using a deque with a maximum length of 200, in a FIFO manner

    [new_value] ---> [deque] ---> [old_value] when maxlen is reached

    :param process_queue: The queue from which this task communicates with the websocket grabbing messages from Coinbase
    :param prod_name: The name of the trading pair
    :param l: A lock for IPC
    """
    try:
        sliding_window = deque(maxlen=WINDOW_SIZE)
        cumulative_numerator, cumulative_denominator = 0, 0
        with open(f"{prod_name}-{datetime.now().strftime('%Y-%m-%d-%H:%M:%S')}.csv", 'w', newline='') as csvfile:
            filewriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            csvfile.write(f"{prod_name} | Match Id | Time | VWAP \n")
            while True:
                val = process_queue.get(block=True, timeout=20)
                out_val = None
                if len(sliding_window) >= WINDOW_SIZE:
                    out_val = sliding_window.pop()
                cumulative_numerator, cumulative_denominator, vwap_val = calculate_vwap(cumulative_numerator,
                                                                                        cumulative_denominator,
                                                                                        val,
                                                                                        out_val)
                sliding_window.appendleft(val)
                filewriter.writerow([val.get('product_id'), val.get('trade_id'), val.get('time'), vwap_val])
                l.acquire()
                print(f"{val.get('product_id')} -  {val.get('trade_id')} at {val.get('time')} has vwap: {vwap_val}")
                l.release()
    except GracefulExit:
        return


def start_process(proc_queue, prod_name, l):
    p = Process(target=vwap_task, args=(proc_queue, prod_name, l))
    p.start()
    return p


def on_press(key, ws, p_list):
    if getattr(key, 'char', None) == 'q':
        [p.terminate() for p in p_list]
        ws.close()
        print("Exiting...")


def usage():
    print("This module utilizes the websocket provided by Coinbase Pro to calculate VWAP for certain trading pairs, "
          "using the matches channel")
    print("Currently outputs VWAP for: ETH-USD, BTC-USD, ETH-BTC")
    print("")
    print("INSTALLATION:")
    print("1. Create a Virtual Environment with 'python -m venv venv'")
    print("2. Source into the venv with 'source venv/bin/activate'")
    print("3. Install dependencies with 'pip install -r requirements.txt'")
    print("")
    print("USAGE")
    print("Run with 'python vwap_module.py'")
    print("You can gracefully exit by pressing 'q'")
    print("")
    print("VWAP values will be identified properly on stdout and also will be written to a csv file.")
    print("File naming convention is <trading_pair>-<datetime_when_running>.csv, for e.g 'BTC-USD-2021-03-29-15:36:09'")
    print("")
    print("TESTING")
    print("Tests can be run with: python -m unittest module_tests")


def parse_arguments():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "h", ["help"])
    except getopt.GetoptError as err:
        print(err)
        usage()
        sys.exit(2)
    for o, a in opts:
        if o in ('-h', '--help'):
            usage()
            sys.exit()


def main():
    parse_arguments()
    signal.signal(signal.SIGTERM, exit_handler)
    product_list = ["ETH-USD", 'BTC-USD', 'ETH-BTC']
    m = Manager()
    vwap_processes_lock = Lock()
    processes_queues = {k: m.Queue() for k in product_list}
    websocket.enableTrace(True)
    msg = {
        "type": "subscribe",
        "product_ids": product_list,
        "channels":
            [
                {
                    "name": "matches",
                }
            ]}
    ws = websocket.WebSocketApp("wss://ws-feed.pro.coinbase.com",
                                on_open=lambda x: on_open(x, msg),
                                on_message=lambda x, y: on_message(x, y, processes_queues),
                                on_error=on_error,
                                on_close=on_close)

    p_list = [start_process(processes_queues[prod], prod, vwap_processes_lock) for prod in product_list]
    keyboard.Listener(on_press=lambda key: on_press(key, ws, p_list)).start()
    ws.run_forever()

if __name__ == "__main__":
    main()

