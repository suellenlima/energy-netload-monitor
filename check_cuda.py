#!/usr/bin/env python
import torch
print(f"CUDA available: {torch.cuda.is_available()}")
print(f"Device count: {torch.cuda.device_count()}")
print(f"PyTorch version: {torch.__version__}")
