import struct
import numpy as np
from src.grib_message import GribMessage

LANLONPARAMS = [
            "min_lat",
            "max_lat",
            "min_lon",
            "max_lon",
            "step_lan",
            "step_lon",
        ]

class MessageParamsMismatch(Exception):
    pass


class Grib2ToWGF4Converter:
    def __init__(
        self,
        message_start: GribMessage,
        message_end: GribMessage,
        output_filename: str = "output.wgf4",
        multiplier: int = 10000,
        empty_value: float = -100500,
    ):
        self.message_start = message_start
        self.message_end = message_end
        self.multiplier = multiplier
        self.empty_value = empty_value
        self.output_filename = output_filename
        self.output_bytes = bytearray()
        self.min_lat = self.min_lon = self.max_lat = self.max_lon = self.step_lan = self.step_lon = 0

    def make_header(self) -> bytearray:
        ret = bytearray()
        for param in LANLONPARAMS:
            ret += struct.pack("i", int(getattr(self, param) * self.multiplier))
        ret += struct.pack("i", self.multiplier)
        return ret

    def to_wgf4(self):
        # Нужно проверить что сообщения охватывают один и тот же квадрат с одним и тем же шагом
        params_to_check = LANLONPARAMS
        for param in params_to_check:
            if getattr(self.message_start, param) != getattr(self.message_end, param):
                raise MessageParamsMismatch(param)
            setattr(self, param, getattr(self.message_start, param))

        self.output_bytes += self.make_header()

        # Записываем empty_value
        self.output_bytes += struct.pack('f', self.empty_value)

        for val_start, val_end in zip(self.message_start.values(), self.message_end.values()):
            if np.ma.is_masked(val_end) or np.ma.is_masked(val_start):
                val = self.empty_value
            else:
                val = val_end - val_start
            self.output_bytes += struct.pack('f', val)

        with open(self.output_filename, 'wb') as f:
            f.write(self.output_bytes)


