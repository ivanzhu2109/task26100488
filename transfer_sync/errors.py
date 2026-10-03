"""异常类型定义。"""


class TransferSyncError(Exception):
    """同步库基础异常。"""


class ConflictError(TransferSyncError):
    """tx_id / batch_id 被以不一致的参数重复使用。"""


class UnknownAccountError(TransferSyncError):
    """账户不属于任何已知网点。"""
