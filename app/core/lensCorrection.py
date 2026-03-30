import numpy as np
from PIL import Image
from scipy.ndimage import map_coordinates
from pathlib import Path
import os
import sys
from app.core.settings import settings

RED_SCALE = settings.RED_SCALE
BLUE_SCALE = settings.BLUE_SCALE
# RED_SCALE = 0.990
# BLUE_SCALE = 1.010

def rescale_channel(channel: np.ndarray, scale: float) -> np.ndarray:
    """
    Radially rescale a single image channel relative to its center.
    scale < 1.0 → shrink (pull pixels inward)
    scale > 1.0 → expand (push pixels outward)
    Green channel is the anchor (scale=1.0, unchanged).
    """
    h, w = channel.shape
    cx, cy = w / 2.0, h / 2.0

    # Build coordinate grids for output pixels
    ys, xs = np.mgrid[0:h, 0:w].astype(np.float64)

    # Map output coords back to source coords via inverse scale
    # (to avoid holes, we do inverse mapping)
    src_x = (xs - cx) / scale + cx
    src_y = (ys - cy) / scale + cy

    # Interpolate source channel at remapped coords
    rescaled = map_coordinates(
        channel.astype(np.float64),
        [src_y, src_x],
        order=3,           # bicubic interpolation
        mode='nearest'     # edge handling
    )

    return np.clip(rescaled, 0, 255).astype(np.uint8)

def create_complete_dir(output_path:str):
    _path = Path(output_path).parent
    if not _path.exists():
        os.mkdir(_path)


def apply_lens_correction(input_path: str, output_path: str):
    create_complete_dir(output_path)

    img = Image.open(input_path).convert('RGB')
    r, g, b = img.split()

    r_arr = np.array(r)
    g_arr = np.array(g)
    b_arr = np.array(b)

    print(f"Processing {input_path} ({img.width}x{img.height})...")

    # Green is the reference — untouched
    r_corrected = rescale_channel(r_arr, RED_SCALE)
    b_corrected = rescale_channel(b_arr, BLUE_SCALE)

    result = Image.merge('RGB', [
        Image.fromarray(r_corrected),
        Image.fromarray(g_arr),      # green unchanged
        Image.fromarray(b_corrected)
    ])

    result.save(output_path, quality=95)
    print(f"Saved -> {output_path}")


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Usage: python lens_correction.py input.jpg output.jpg")
        sys.exit(1)
    apply_lens_correction(sys.argv[1], sys.argv[2])