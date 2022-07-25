#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Functions to get image size."""

import struct
from pathlib import Path
from typing import Tuple

from _io import BufferedReader

from graviti.exception import ImageDecodeError

try:
    from PIL import Image
except ModuleNotFoundError:
    from graviti.utility.common import ImageMocker  # pylint:disable=ungrouped-imports

    Image = ImageMocker(
        "Only support getting the size of "
        "JPEG, PNG, JPEG2000, GIF, BMP, GIF, TIFF, ICO, WebP, Flif formats. "
        "Please install pillow to process this file"
    )


def get_image_size(path: Path) -> Tuple[int, int]:
    """Get the height and width of the input image file.

    Arguments:
        path: The path of the image.

    Returns:
        The height and width of the input image.

    """
    with path.open("rb") as fp:
        header = fp.read(26)
        fp.seek(0)
        for image_format in ImageFormatBase.__subclasses__():
            if image_format.check(header, path.stat().st_size):
                return image_format.get_image_size(header, fp)

    return Image.open(path).size  # type: ignore[no-any-return]


class ImageFormatBase:
    """The base class for different image formats."""

    @classmethod
    def check(cls, header: bytes, size: int) -> bool:
        """Check if the input header fits the current image format.

        Arguments:
            header: The header of the image.
            size: The size of the image.

        Returns:
            Whether if the input header fits the current image format.

        """
        return cls._check(header, size)

    @classmethod
    def get_image_size(cls, header: bytes, fp: BufferedReader) -> Tuple[int, int]:
        """Get the height and width through the input data.

        Arguments:
            header: The header of the image or the entire image.
            fp: The image file pointer.

        Returns:
            The height and width of the image.

        Raises:
            ImageDecodeError: When the input image file is invalid.

        """
        try:
            return cls._get_image_size(header, fp)
        except (struct.error, IndexError, ValueError, TypeError):
            raise ImageDecodeError(f"Invalid {cls.__name__} file") from None

    @staticmethod
    def _get_image_size(header: bytes, fp: BufferedReader) -> Tuple[int, int]:
        raise NotImplementedError

    @staticmethod
    def _check(header: bytes, size: int) -> bool:
        raise NotImplementedError


class JPEG(ImageFormatBase):
    """The class for JPEG image format."""

    @staticmethod
    def _check(header: bytes, size: int) -> bool:
        return size >= 2 and header.startswith(b"\377\330")

    @staticmethod
    def _get_image_size(header: bytes, fp: BufferedReader) -> Tuple[int, int]:
        size = 2
        ftype = 0
        while not 0xC0 <= ftype <= 0xCF or ftype in {0xC4, 0xC8, 0xCC}:
            fp.seek(size, 1)
            byte = fp.read(1)
            while ord(byte) == 0xFF:
                byte = fp.read(1)
            ftype = ord(byte)
            size = struct.unpack(">H", fp.read(2))[0] - 2
        # We are at a SOFn block
        fp.seek(1, 1)  # Skip `precision' byte.
        height, width = struct.unpack(">HH", fp.read(4))
        return height, width


class PNG(ImageFormatBase):
    """The class for PNG image format."""

    @staticmethod
    def _check(header: bytes, size: int) -> bool:
        return size >= 24 and header[1:4] == b"PNG" and header[12:16] == b"IHDR"

    @staticmethod
    def _get_image_size(header: bytes, fp: BufferedReader) -> Tuple[int, int]:
        width, height = struct.unpack(">LL", header[16:24])
        return height, width


class OldPNG(ImageFormatBase):
    """The class for an older version of PNG image format."""

    @staticmethod
    def _check(header: bytes, size: int) -> bool:
        return size >= 16 and header[1:4] == b"PNG"

    @staticmethod
    def _get_image_size(header: bytes, fp: BufferedReader) -> Tuple[int, int]:
        width, height = struct.unpack(">LL", header[8:16])
        return height, width


class GIF(ImageFormatBase):
    """The class for GIF image format."""

    @staticmethod
    def _check(header: bytes, size: int) -> bool:
        return size >= 10 and header[:6] in {b"GIF87a", b"GIF89a"}

    @staticmethod
    def _get_image_size(header: bytes, fp: BufferedReader) -> Tuple[int, int]:
        width, height = struct.unpack("<HH", header[6:10])
        return height, width


class JPEG2000(ImageFormatBase):
    """The class for JPEG 2000 image format."""

    @staticmethod
    def _check(header: bytes, size: int) -> bool:
        return size >= 12 and header.startswith(b"\x00\x00\x00\x0cjP  \r\n\x87\n")

    @staticmethod
    def _get_image_size(header: bytes, fp: BufferedReader) -> Tuple[int, int]:
        fp.seek(48)
        height, width = struct.unpack(">LL", fp.read(8))
        return height, width


class BMP(ImageFormatBase):
    """The class for BMP image format."""

    @staticmethod
    def _check(header: bytes, size: int) -> bool:
        return size >= 26 and header[:2] == b"BM"

    @staticmethod
    def _get_image_size(header: bytes, fp: BufferedReader) -> Tuple[int, int]:
        headersize = struct.unpack("<I", header[14:18])[0]
        if headersize == 12:
            width, height = struct.unpack("<HH", header[18:22])
        elif headersize >= 40:
            width, height = struct.unpack("<ii", header[18:26])
            height = abs(height)  # height is inverted, so abs() the result
        else:
            raise ImageDecodeError("Invalid BMP file")
        return height, width


class TIFF(ImageFormatBase):
    """The class for TIFF image format."""

    _TIFF_TYPES = {
        byte_order_sign: {
            1: (1, byte_order_sign + "B"),  # BYTE
            2: (1, byte_order_sign + "c"),  # ASCII
            3: (2, byte_order_sign + "H"),  # SHORT
            4: (4, byte_order_sign + "L"),  # LONG
            5: (8, byte_order_sign + "LL"),  # RATIONAL
            6: (1, byte_order_sign + "b"),  # SBYTE
            7: (1, byte_order_sign + "c"),  # UNDEFINED
            8: (2, byte_order_sign + "h"),  # SSHORT
            9: (4, byte_order_sign + "l"),  # SLONG
            10: (8, byte_order_sign + "ll"),  # SRATIONAL
            11: (4, byte_order_sign + "f"),  # FLOAT
            12: (8, byte_order_sign + "d"),  # DOUBLE
        }
        for byte_order_sign in (">", "<")
    }

    @staticmethod
    def _check(header: bytes, size: int) -> bool:
        return size >= 8 and header[:4] in {b"II\052\000", b"MM\000\052"}

    @staticmethod
    def _get_image_size(header: bytes, fp: BufferedReader) -> Tuple[int, int]:
        # Standard TIFF, big- or little-endian
        # BigTIFF and other different but TIFF-like formats are not
        # supported currently
        byte_order_sign = ">" if header[:2] == b"MM" else "<"
        # maps TIFF type id to size (in bytes)
        # and python format char for struct
        tiff_types = TIFF._TIFF_TYPES[byte_order_sign]
        ifd_offset = struct.unpack(byte_order_sign + "L", header[4:8])[0]
        fp.seek(ifd_offset)
        # 2 bytes: TagId + 2 bytes: type + 4 bytes: count of values + 4
        # bytes: value offset
        width, height = -1, -1
        digit_type = byte_order_sign + "H"
        for i in range(struct.unpack(digit_type, fp.read(2))[0]):
            entry_offset = ifd_offset + 2 + i * 12
            fp.seek(entry_offset)
            tag = struct.unpack(digit_type, fp.read(2))[0]
            if tag in {256, 257}:
                # if type indicates that value fits into 4 bytes, value
                # offset is not an offset but value itself
                type_ = struct.unpack(digit_type, fp.read(2))[0]
                try:
                    type_size, type_char = tiff_types[type_]
                except KeyError:
                    raise ImageDecodeError(f"Unkown TIFF field type: {type_}") from None
                fp.seek(entry_offset + 8)
                value = int(struct.unpack(type_char, fp.read(type_size))[0])
                if tag == 256:
                    width = value
                else:
                    height = value
            if width > -1 and height > -1:
                break

        if width == -1 or height == -1:
            raise ImageDecodeError(
                "Invalid TIFF file: width and/or height IDS entries are missing."
            )

        return height, width


class ICO(ImageFormatBase):
    """The class for ICO image format."""

    @staticmethod
    def _check(header: bytes, size: int) -> bool:
        return (
            size >= 2
            and struct.unpack("<H", header[:2])[0] == 0
            and struct.unpack("<H", header[2:4])[0] == 1
        )

    @staticmethod
    def _get_image_size(header: bytes, fp: BufferedReader) -> Tuple[int, int]:
        # read the dimensions of the first image
        return header[7], header[6]


class WebP(ImageFormatBase):
    """The class for WebP image format."""

    @staticmethod
    def _check(header: bytes, size: int) -> bool:
        return size >= 24 and header[:4] == b"RIFF" and header[8:12] == b"WEBP"

    @staticmethod
    def _get_image_size(header: bytes, fp: BufferedReader) -> Tuple[int, int]:
        signature = header[12:16]
        if signature == b"VP8L":
            digits = struct.unpack("4B", header[21:25])
            width = 1 + (((digits[1] & 0b111111) << 8) | digits[0])
            height = 1 + (
                ((digits[3] & 0b1111) << 10) | (digits[2] << 2) | ((digits[1] & 0b11000000) >> 6)
            )
        elif signature == b"VP8 ":
            sc_a, sc_b, sc_c = struct.unpack("3B", header[23:26])
            if sc_a != 0x9D or sc_b != 0x01 or sc_c != 0x2A:
                raise ImageDecodeError("Missing start code block for lossy WebP image")
            width, height = struct.unpack("<HH", fp.read(4))
        elif signature == b"VP8X":
            width, height = struct.unpack("<HxH", header[24:] + fp.read(3))
            width, height = width + 1, height + 1
        else:
            raise ImageDecodeError("Invalid WebP file")
        return height, width


class FLIF(ImageFormatBase):
    """The class for Flif image format."""

    @staticmethod
    def _check(header: bytes, size: int) -> bool:
        return size >= 16 and header[:4] == b"FLIF"

    @staticmethod
    def _get_image_size(header: bytes, fp: BufferedReader) -> Tuple[int, int]:
        width, size = FLIF._read_varint(header[6:])
        height, _ = FLIF._read_varint(header[6 + size :])
        return height + 1, width + 1

    @staticmethod
    def _read_varint(data: bytes) -> Tuple[int, int]:
        values = []
        for byte in data:
            value = byte & 0b01111111
            has_leading_bit = byte & 0b10000000
            values.append(value)
            if not has_leading_bit:
                break

        result = 0
        for i, value in enumerate(reversed(values)):
            result |= value << (i * 7)

        return result, len(values)
