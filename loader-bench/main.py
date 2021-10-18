#!/usr/bin/env python3
import argparse
import io
import os
import time
from typing import Any, Tuple

import numpy as np
import pfio
import torch
from torch.utils.data import Dataset
from torch.utils.data.dataloader import DataLoader


def index_to_rgb(index: int) -> Tuple[int, int, int]:
    return index & 255, (index >> 8) & 255, (index >> 16) & 255


def rgb_to_index(rgb: Any) -> int:
    r, g, b = int(rgb[0]), int(rgb[1]), int(rgb[2])
    return r | (g << 8) | (b << 16)


def get_dummy_dataset(dataset_len: int, cache_path: str, cache_type: str) -> Any:
    return DummySemSegDataset(dataset_len, cache_path, cache_type)


class DummySemSegDataset(Dataset):
    def __init__(self, dataset_len: int, cache_path: str, cache_type: str) -> None:
        self._dataset_len = dataset_len
        cache_dir = os.path.dirname(cache_path)
        if cache_type == "naive":
            cls = pfio.cache.NaiveCache
        elif cache_type == "file":
            cls = pfio.cache.FileCache
        elif cache_type == "multiprocess":
            cls = pfio.cache.MultiprocessFileCache
        elif cache_type == "readonly":
            cls = pfio.cache.ReadOnlyFileCache
        else:
            raise ValueError(cache_type)
        print("Cache class:", cls)
        self._cache = cls(self._dataset_len, dir=cache_dir)

    def _get_dummy_data(self, i: int) -> bytes:
        img = np.ones((1280, 720, 3), dtype=np.uint8)
        label = np.ones((1280, 720), dtype=np.uint8)
        img[0, 0, :] = index_to_rgb(i)
        ret = io.BytesIO()
        # Loading data in .npz format is much slower than that of .npy format.
        # The main overhead comes from `zlib.crc32` according to cProfile.
        np.save(ret, img)
        np.save(ret, label)
        return ret.getvalue()

    def __getitem__(self, index: int) -> Tuple[Any, Any, float]:
        b_s = time.time()
        data = self._cache.get_and_cache(index, self._get_dummy_data)
        e_s = time.time()
        data_stream = io.BytesIO(data)
        img = np.load(data_stream)
        label = np.load(data_stream)
        return img, label, e_s - b_s

    def __len__(self) -> int:
        return self._dataset_len


def benchmark(loader: Any) -> float:
    etime_accum = 0.0
    for _, _, etime in loader:
        etime_accum += float(torch.sum(etime))
    return etime_accum


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset-len", default=1024, type=int)
    parser.add_argument("--batchsize", default=32, type=int)
    parser.add_argument("--num-workers", default=8, type=int)
    parser.add_argument("--num-trials", default=5, type=int)
    parser.add_argument(
        "--preload-path", type=str, help="MultiprocessFileCache.preload"
    )
    parser.add_argument(
        "--preserve-path", type=str, help="MultiprocessFileCache.preserve"
    )
    parser.add_argument(
        "--cache-type", "-c", type=str, choices=["naive", "file", "multiprocess", "readonly",],
        default="multiprocess", help="Choose cache class; some are RO")
    args = parser.parse_args()

    if args.dataset_len >= 2 ** 24:
        print("Too long --dataset-len")
        exit()

    def xor(lhs: str, rhs: str) -> int:
        return (1 if lhs else 0) ^ (1 if rhs else 0)

    if not xor(args.preload_path, args.preserve_path):
        print("Specify either --preload-path or --preserve-path")
        exit()

    if args.preload_path:
        if not os.path.exists(args.preload_path):
            print(f"{args.preload_path} does not exist.")
            exit()
        cache_path = args.preload_path

    if args.preserve_path:
        if os.path.exists(args.preserve_path):
            print(f"{args.preserve_path} already exists.")
            exit()
        if args.cache_type != "multiprocess":
            print("To preserve cache, cache type must be MP-safe")
            exit()
        cache_path = args.preserve_path

    dummy_dataset = get_dummy_dataset(args.dataset_len, cache_path,
                                      args.cache_type)

    if args.preload_path:
        dummy_dataset._cache.preload(args.preload_path)

    loader = DataLoader(
        dummy_dataset,
        batch_size=args.batchsize,
        drop_last=True,
        num_workers=args.num_workers,
        pin_memory=True,
        shuffle=True,
        persistent_workers=True,
    )

    if 1:
        print(args)
        for n in range(args.num_trials):
            b_s = time.time()
            etime_accum_s = benchmark(loader)
            e_s = time.time()
            etime_all_s = e_s - b_s
            ave_etime_img_label_ms = etime_accum_s / args.dataset_len * 1000.0
            print(
                f"trial: {n + 1:2}, "
                f"elapsed-time(all): {etime_all_s:.3f} sec, "
                f"ave-elapsed-time(img+label): {ave_etime_img_label_ms:.3f} msec"
            )
    else:
        # For debug
        for n in range(args.num_trials):
            print(f"{n} th")
            indices = set()
            for img, _ in loader:
                for batch in range(args.batchsize):
                    rgb = img[batch, 0, 0, :]
                    indices.add(rgb_to_index(rgb))
            assert len(indices) == args.dataset_len, "Mismatch"

    if args.preserve_path:
        dummy_dataset._cache.preserve(args.preserve_path, overwrite=False)


if __name__ == "__main__":
    main()
