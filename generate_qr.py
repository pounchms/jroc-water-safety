"""
QR Code Generator for JROC Near-Miss Reporting Form

Usage:
    python generate_qr.py "https://forms.gle/YOUR_FORM_URL" jroc_near_miss_qr.png

Generates a print-ready QR code PNG. Pass the final Google Form URL
after the form has been created.

Install: pip install qrcode[pil]
"""

import sys
import qrcode
from qrcode.image.styledpil import StyledPilImage
from qrcode.image.styles.moduledrawers import RoundedModuleDrawer


def generate_qr(url: str, output_file: str = "jroc_near_miss_qr.png"):
    qr = qrcode.QRCode(
        version=None,
        error_correction=qrcode.constants.ERROR_CORRECT_H,  # high error correction for outdoor signs
        box_size=10,
        border=4,
    )
    qr.add_data(url)
    qr.make(fit=True)

    img = qr.make_image(
        image_factory=StyledPilImage,
        module_drawer=RoundedModuleDrawer(),
    )
    img.save(output_file)
    print(f"QR code saved: {output_file}")
    print(f"URL encoded: {url}")
    print(f"Size: {img.pixel_size}x{img.pixel_size}px")
    print("\nPrint at 3x3 inches minimum for reliable outdoor scanning.")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python generate_qr.py <form_url> [output_file.png]")
        sys.exit(1)

    url = sys.argv[1]
    output = sys.argv[2] if len(sys.argv) > 2 else "jroc_near_miss_qr.png"
    generate_qr(url, output)
