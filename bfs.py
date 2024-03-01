from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Manager
import time 

from tqdm import tqdm 

from parsers import AVAILABLE

IN_F = '/mnt/raid1_ssd_4tb/datasets/financial_gan/babd_dataset/BABD-13.csv'
OUT_F = 'edges.csv'

COOLDOWN = 2
WORKERS = 16

FILE_WRITE_LOCK =  Manager().Lock()
PROG = tqdm()

def thread_job(i,wallet):    
    st = time.time() 
    
    # Alternate which API we hit
    parser = AVAILABLE[i % len(AVAILABLE)]
    (src,dst), w,t = parser(wallet)

    rows = zip(src,dst,w,t)
    out_str = ''
    for r in rows:
        out_str += ','.join([str(r_) for r_ in r]) + '\n'

    with FILE_WRITE_LOCK:
        with open(OUT_F, 'a') as f:
            f.write(out_str)
        PROG.update()

    elapsed = time.time()-st 
    time.sleep(max(COOLDOWN - elapsed,0))


def main():
    wallets = []
    with open(IN_F, 'r') as f:
        f.readline() # Skip header
        line = f.readline() 
        while line:
            wallets.append(line.split(',')[0])
            line = f.readline() 
    
    # Create new empty out file 
    with open(OUT_F, 'w+'):
        pass 

    PROG.total = len(wallets)
    with ThreadPoolExecutor(max_workers=WORKERS) as tp:
        tp.map(thread_job, wallets)

st = time.time()
main()
elapsed = time.time() - st
print(f'Done! ({elapsed}s)')