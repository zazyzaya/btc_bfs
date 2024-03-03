import os 
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Manager
import time 

from tqdm import tqdm 

from parsers import AVAILABLE

IN_F = '/mnt/raid1_ssd_4tb/datasets/financial_gan/babd_dataset/BABD-13.csv'
OUT_DIR = 'iters'
OUT_F = ''

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


def first_hop():
    global OUT_F 
    OUT_F = f'{OUT_DIR}/1_edges.csv'

    wallets = []
    with open(IN_F, 'r') as f:
        f.readline() # Skip header
        line = f.readline() 
        while line:
            lable = int(line.split(',')[-1].strip())
            
            # Only interested in blacklisted, laundering, or ponzi schemes
            if lable in [7,8,9]:
                wallets.append(line.split(',')[0])
            line = f.readline() 
    
    # Create new empty out file 
    with open(OUT_F, 'w+'):
        pass 

    PROG.total = len(wallets)

    # Builds out edge file
    with ThreadPoolExecutor(max_workers=WORKERS) as tp:
        for i,w in enumerate(wallets):
            tp.submit(thread_job, i,w)
    
    with open(f'{OUT_DIR}/1_explored.csv', 'w+') as f:
        f.write('\n'.join(wallets))

def kth_hop(k):
    global OUT_F
    OUT_F = f'{OUT_DIR}/{k}_edges.csv'

    with open(f'{OUT_DIR}/{k-1}_explored.csv', 'r') as f:
        explored = set(
            f.read().split('\n')
        )

    domain = set() 
    with open(f'{OUT_DIR}/{k-1}_edges.csv', 'r') as f:
        line = f.readline()
        while line:
            src,dst,_ = line.split(',', 2)
            if src not in explored:
                domain.add(src)
            if dst not in explored:
                domain.add(dst)
            
            line = f.readline()

    PROG.total = len(domain)

    # Builds out edge file
    with ThreadPoolExecutor(max_workers=WORKERS) as tp:
        for i,w in enumerate(domain):
            tp.submit(thread_job, i,w)
    
    with open(f'{OUT_DIR}/{k}_explored.csv', 'w+') as f:
        f.write('\n'.join(domain.union(explored)))

if __name__ == '__main__':
    st = time.time()
    kth_hop(2)
    elapsed = time.time() - st
    print(f'2nd hop completed in {elapsed}s')