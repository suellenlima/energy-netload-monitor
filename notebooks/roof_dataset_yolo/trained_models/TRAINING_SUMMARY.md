
# YOLO Roof Detection Model - Training Summary

## Dataset Statistics
- Total samples: 200
- Training samples: 139
- Validation samples: 40
- Test samples: 21

## Model Configuration
- Model: YOLOv8 Nano
- Input size: 640x640
- Classes: roof
- Number of classes: 1

## Training Parameters
- Epochs: 50
- Batch size: 8
- Device: CPU

## Model Locations
- PyTorch: /home/jovyan/work/roof_dataset_yolo/trained_models/best.pt
- ONNX: /home/jovyan/work/roof_dataset_yolo/trained_models/best.onnx
- TorchScript: /home/jovyan/work/roof_dataset_yolo/trained_models/best.torchscript.pt

## Training Results
- Validation mAP50: 0.8130
- Validation mAP50-95: 0.5266
- Test mAP50: 0.9146
- Test mAP50-95: 0.6642

## Dataset Path
/home/jovyan/work/roof_dataset_yolo

## Training Runs Path
runs/detect/runs/roof_detection/yolov8n_finetuned
