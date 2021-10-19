# DataLoader benchmark of ppe-semseg

## Quick start

[Optional] Prepare [venv](https://docs.python.org/3/library/venv.html) not to make your environment dirty

```sh
$ python3 -m venv bench
$ source bench/bin/activate
```

Run micro-benchmark

```sh
$ cd pfio
$ pip install -U -e .[bench]
$ cd loader-bench
$ time python3 main.py --preserve-path=/tmp/dat
# Expected output example
Namespace(batchsize=32, dataset_len=1024, num_trials=5, num_workers=8, preload_path=None, preserve_path='/tmp/dat')
trial:  1, elapsed-time(all): 10.952 sec, ave-elapsed-time(img+label): 78.390 msec
trial:  2, elapsed-time(all): 1.859 sec, ave-elapsed-time(img+label): 6.178 msec
trial:  3, elapsed-time(all): 1.780 sec, ave-elapsed-time(img+label): 5.455 msec
trial:  4, elapsed-time(all): 1.788 sec, ave-elapsed-time(img+label): 5.080 msec
trial:  5, elapsed-time(all): 1.723 sec, ave-elapsed-time(img+label): 4.456 msec
python3 main.py --preserve-path /tmp/dat  32.56s user 24.93s system 304% cpu 18.858 total
$ time python3 main.py --preload-path=/tmp/dat  # If `/tmp/dat` already exists
```

The size of the preserved data should be around 3.6 GB when `dataset_len`=1024.

## Generate smaller/larger dummy dataset

`dataset_len` should be set to the multiples of `batchsize`.

```sh
$ export BATCHSIZE=19
$ export DATASET_LEN=76
$ python3 main.py --preserve-path=/tmp/dat_small --batchsize=${BATCHSIZE} --dataset_len=${DATASET_LEN}
```
