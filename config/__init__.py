"""
config 패키지 — 하위 모듈을 전부 re-export합니다.

기존 ``from config import X`` 구문이 수정 없이 동작합니다.
파이프라인별로 ``from config.ir_use import X``, ``from config.simple import X``
처럼 명시적으로 import할 수도 있습니다.
"""

from .common import *   # noqa: F401,F403
from .ir_use import *   # noqa: F401,F403
from .simple import *   # noqa: F401,F403
